import datetime

from django.db import models
from django.contrib.auth.models import User

from g4mer.utils.test import TestCase
from g4mer.db.models.fields import JSONField

from .fields import CycleBufferField

class CycleBufferTests(TestCase):
    def getModels(self):
        class Model(models.Model):
            text_buffer = CycleBufferField(
                cycle_size=3,
                slot=models.TextField(),
            )

            date_buffer = CycleBufferField(
                cycle_size=3,
                slot=models.DateField(null=True),
            )

            class Meta:
                app_label = 'cb_tests'

        class FKModel(models.Model):
            user_buffer = CycleBufferField(
                cycle_size=3,
                slot=models.ForeignKey(User, null=True),
            )

            class Meta:
                app_label = 'cb_tests'

        class Model2(models.Model):
            text_buffer = CycleBufferField(
                cycle_size=3,
                slot=models.TextField(default='hello'),
            )

            class Meta:
                app_label = 'cb_tests'

        class ModelJSON(models.Model):
            json_buffer = CycleBufferField(
                cycle_size=3,
                slot=JSONField(default={}),
            )

            class Meta:
                app_label = 'cb_tests'

        return (Model, FKModel, Model2, ModelJSON)

    def testNonNull(self):
        m = self.models.Model2()
        self.assertEqual(m.text_buffer, [])
        m.text_buffer.append_object('first')
        self.assertEqual(m.text_buffer, ['first'])

        m = self.models.Model2.objects.create()
        m.text_buffer.append_database('first')
        m = self.models.Model2.objects.get(pk=m.pk)
        self.assertEqual(m.text_buffer, ['first'])

    def testJSON(self):
        m = self.models.ModelJSON()
        self.assertEqual(m.json_buffer, [])
        m.json_buffer.append_object({'foo': 'bar'})
        self.assertEqual(m.json_buffer, [{'foo': 'bar'}])

        m = self.models.ModelJSON.objects.create()
        m.json_buffer.append_database({'foo': 'bar'})
        m = self.models.ModelJSON.objects.get(pk=m.pk)
        self.assertEqual(m.json_buffer, [{'foo': 'bar'}])

    def testCycleBuffer(self):
        m = self.models.Model()

        self.assertEqual(m.text_buffer, [])

        mapping = (
            ('a', ['a']),
            ('b', ['a', 'b']),
            ('c', ['a', 'b', 'c']),
            ('d', ['b', 'c', 'd']),
            ('e', ['c', 'd', 'e']),
            ('f', ['d', 'e', 'f']),
            ('g', ['e', 'f', 'g']),
            ('h', ['f', 'g', 'h']),
            ('i', ['g', 'h', 'i']),
            ('j', ['h', 'i', 'j']),
        )

        for append, expected in mapping:
            m.text_buffer.append_object(append)
            m.save()
            self.assertEqual(m.text_buffer, expected)

        m = self.models.Model.objects.create()
        for append, expected in mapping:
            m.text_buffer.append_database(append)
            m = self.models.Model.objects.get(pk=m.pk)
            self.assertEqual(m.text_buffer, expected)

        m.date_buffer.append_object(datetime.date(2009, 03, 26))
        m.save()
        self.assertEqual(m.date_buffer, [datetime.date(2009, 03, 26)])
        m.date_buffer.append_object(datetime.date(2009, 03, 27))
        m.save()
        self.assertEqual(
            m.date_buffer,
            [datetime.date(2009, 03, 26), datetime.date(2009, 03, 27)],
        )

        m = self.models.Model.objects.create()
        m.date_buffer.append_database(datetime.date(2009, 03, 26))
        m = self.models.Model.objects.get(pk=m.pk)
        self.assertEqual(m.date_buffer, [datetime.date(2009, 03, 26)])
        m.date_buffer.append_database(datetime.date(2009, 03, 27))
        m = self.models.Model.objects.get(pk=m.pk)
        self.assertEqual(
            m.date_buffer, [
            datetime.date(2009, 03, 26), datetime.date(2009, 03, 27)],
        )

    def testCycleBufferForeignKeys(self):
        m = self.models.FKModel()
        self.user1 = self.makeUser("testuser1")
        self.user2 = self.makeUser("testuser2")
        self.user3 = self.makeUser("testuser3")
        self.user4 = self.makeUser("testuser4")

        self.assertEqual(m.user_buffer, [])
        m.user_buffer.append_object(self.user1)
        m.save()
        self.assertEqual(m.user_buffer, [self.user1])
        m.user_buffer.append_object(self.user2)
        m.save()
        for u in [self.user2, self.user3, self.user4]:
            m.user_buffer.append_object(u)
        m.save()

        self.assertEqual(m.user_buffer, [self.user2, self.user3, self.user4])

    def testCycleBufferForeignKeysDb(self):
        m = self.models.FKModel.objects.create()
        self.user1 = self.makeUser("testuser1")
        self.user2 = self.makeUser("testuser2")
        self.user3 = self.makeUser("testuser3")
        self.user4 = self.makeUser("testuser4")

        self.assertEqual(m.user_buffer, [])
        m.user_buffer.append_database(self.user1)

        m = self.models.FKModel.objects.get(pk=m.pk)
        self.assertEqual(m.user_buffer, [self.user1])

        m.user_buffer.append_database(self.user2)
        m = self.models.FKModel.objects.get(pk=m.pk)

        for u in [self.user2, self.user3, self.user4]:
            m.user_buffer.append_database(u)

        m = self.models.FKModel.objects.get(pk=m.pk)
        self.assertEqual(m.user_buffer, [self.user2, self.user3, self.user4])
