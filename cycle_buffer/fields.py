import copy
import itertools

from django.db import models, connection

class CycleBufferField(models.Field):
    """
    Aggregate field that manages 'n' arbitrary Django fields in a fixed-size
    ring buffer implemented across a number of database columns. Items are
    appended atomically in a single database query.

    Appending a value puts that value on the head and pushes the remaining
    items down one, removing the last item.

    The primary use-case is to store data such as "X recent actions" without
    having to worry about garbage collection of old items, etc.

    Almost any Django field can be used as the prototype field, but they must
    set ``null=True`` or a suitable default value or Django will not be able to
    create any model instances. ``ForeignKey`` fields are supported (and are
    even optimised for by when retrieving the related objects in a single
    database lookup) but they may imply a database constraint and index for
    each slot in the buffer depending on your database backend.

    Example usage::

        class Test(Model):
            nums = CycleBufferField(IntegerField(default=0), cycle_size=2))

        >>> x = Test.objects.create()
        >>> x.nums
        []
        >>> x.nums.append_object(1)
        >>> x.nums
        [1]
        >>> x.nums.append_object(2)
        >>> x.nums
        [1, 2]
        >>> x.nums.append_object(3)
        >>> x.nums
        [2, 3]
    """

    def __init__(self, slot, cycle_size, *args, **kwargs):
        self.slot = slot
        self.cycle_size = cycle_size
        super(CycleBufferField, self).__init__(*args, **kwargs)

    def db_type(self, connection=None):
        return None

    def contribute_to_class(self, cls, name):
        ptr_field = models.IntegerField(default=0)
        ptr_field.creation_counter = self.creation_counter
        cls.add_to_class('%s_ptr' % name, ptr_field)

        len_field = models.IntegerField(default=0)
        len_field.creation_counter = self.creation_counter
        cls.add_to_class('%s_len' % name, len_field)

        for i in range(self.cycle_size):
            slot_field = copy.deepcopy(self.slot)
            slot_field.creation_counter = self.creation_counter
            slot_field.db_column = '%s_%d' % (name, i)

            if self.slot.__class__ is models.fields.related.ForeignKey:
                base = slot_field.rel.related_name or name
                slot_field.rel.related_name = '%s_%d' % (base, i + 1)

            cls.add_to_class('%s_%d' % (name, i), slot_field)

        # Don't call super, we don't want an actual field of name "name".
        self.set_attributes_from_name(name)

        setattr(cls, self.name, CycleBufferDescriptor(self))

class CycleBuffer(list):
    def __init__(self, des, obj, slots):
        self.des = des
        self.obj = obj

        super(CycleBuffer, self).__init__(slots)

    def append_database(self, value):
        """
        Append ``value`` to this CycleBuffer directly and atomically in the
        database; the equivalent of ``django.db.models.expressions.F``.
        """

        qn = connection.ops.quote_name

        common = {
            'db_table': qn(self.obj._meta.db_table),
            'pk_field': qn(self.obj._meta.pk.column),
            'ptr_field': qn(self.des.pointer_field),
            'len_field': qn(self.des.len_field),
            'cycle_size': self.des.field.cycle_size,
        }

        def gen_values():

            for idx, field in enumerate(self.des.buffer_entries):
                args = {
                    'idx': idx,
                    'field': field,
                }
                args.update(common)

                yield '%(field)s = CASE (%(ptr_field)s + %(len_field)s) %%%% ' \
                    '%(cycle_size)s WHEN %(idx)d THEN %%s ELSE %(field)s END' % args

            yield '%(ptr_field)s = CASE %(len_field)s WHEN %(cycle_size)s ' \
                'THEN (%(ptr_field)s + 1) %%%% %(cycle_size)s ELSE ' \
                '%(ptr_field)s END' % common
            yield '%(len_field)s = CASE %(len_field)s + 1 > %(cycle_size)d ' \
                'WHEN 1 THEN %(cycle_size)d ELSE %(len_field)s + 1 END' % common

        common['values'] = ', '.join(gen_values())
        sql = 'UPDATE %(db_table)s SET %(values)s WHERE %(pk_field)s = %%s' % common

        if isinstance(value, models.base.Model):
            value = value.pk

        args = [self.des.field.slot.get_db_prep_save(value, connection=connection)] * \
            self.des.field.cycle_size
        args.append(self.obj.pk)

        connection.cursor().execute(sql, tuple(args))

    def append_object(self, value):
        """
        Append ``value`` to this CycleBuffer, but do not adjust the database.

        You will have to call ``instance.save()`` to persist the changes
        performed by this method.
        """

        head_ptr = getattr(self.obj, self.des.pointer_field)
        buffer_len = getattr(self.obj, self.des.len_field)

        target = (head_ptr + buffer_len) % self.des.field.cycle_size
        new_buffer_len = min(buffer_len + 1, self.des.field.cycle_size)

        if buffer_len == self.des.field.cycle_size:
            head_ptr = (head_ptr + 1) % self.des.field.cycle_size
        else:
            head_ptr = head_ptr

        setattr(self.obj, self.des.buffer_entries[target], value)
        setattr(self.obj, self.des.pointer_field, head_ptr)
        setattr(self.obj, self.des.len_field, new_buffer_len)

    def fields(self):
        return [self.des.pointer_field, self.des.len_field] + \
            self.des.buffer_entries

class CycleBufferDescriptor(object):
    def __init__(self, field):
        self.field = field
        self.buffer_entries = []
        self.pointer_field = '%s_ptr' % self.field.name
        self.len_field = '%s_len' % self.field.name

        for i in range(self.field.cycle_size):
            slot_name = '%s_%d' % (self.field.name, i)
            self.buffer_entries.append(slot_name)

    def __get__(self, obj, type=None):
        if obj is None:
            raise AttributeError("Can only be called on an instance.")

        ptr = getattr(obj, self.pointer_field)
        buffer_len = getattr(obj, self.len_field)

        indexes = list(itertools.chain(
            self.buffer_entries[ptr:ptr + buffer_len],
            self.buffer_entries[:ptr],
        ))

        result = [getattr(obj, x) for x in indexes]

        # Optimisation to retrieve all foreign key objects in a single query.
        if self.field.slot.__class__ is models.fields.related.ForeignKey:
            pks = [getattr(obj, '%s_id' % x) for x in indexes]
            bulk = self.field.slot.rel.to.objects.in_bulk(pks)
            result = [bulk.get(pk, None) for pk in pks]

        return CycleBuffer(self, obj, result)

    def __set__(self, obj, value):
        pass
