
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

import pyschema
import pyschema.core
import pyschema_extensions.avro

from luigi.format import Format



class PySchemaAvroOutputProcessor(object):
    def __init__(self, output_pipe, avro_schema):
        self.avro_writer = DataFileWriter(output_pipe, DatumWriter(), avro_schema)

    def write(self, record):
        record_as_dict = pyschema.core.to_json_compatible(record)
        self.avro_writer.append(record_as_dict)

    def close(self):
        self.avro_writer.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type:
            # pass on the exception; it get's handled in `atomic_file`:
            # http://git.io/x7EVtQ
            return False
        self.close()

class PySchemaAvroInputProcessor(object):
    def __init__(self, input_pipe, schema_class):
        self.input_pipe = input_pipe
        self.schema_class = schema_class

        self.avro_reader = DataFileReader(input_pipe, DatumReader())

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.input_pipe.close()

    def __iter__(self):
        for row in self.avro_reader:
            yield pyschema.core.from_json_compatible(
                schema=self.schema_class,
                dct=row
            )


class PySchemaAvroFormat(Format):
    def __init__(self, schema_class):
        schema_json = pyschema_extensions.avro.get_schema_string(schema_class)

        self.avro_schema = avro.schema.parse(schema_json)
        self.schema_class = schema_class

    def pipe_reader(self, input_pipe):
        return PySchemaAvroInputProcessor(input_pipe, self.schema_class)

    def pipe_writer(self, output_pipe):
        return PySchemaAvroOutputProcessor(output_pipe, self.avro_schema)



if __name__ == "__main__":
    from luigi import Task, LocalTarget, build
    from time import time

    class Target(pyschema.Record):
        impressions = pyschema.Integer()
        clicks = pyschema.Integer()

    class Actual(pyschema.Record):
        target = pyschema.SubRecord(Target)
        piet = pyschema.Text()


    class A(Task):
        def output(self):
            return LocalTarget("/tmp/blaat", format=PySchemaAvroFormat(Actual))

        def run(self):
            c = 100000
            actual = Actual(target=Target(impressions=10, clicks=20), piet="123")

            start = time()
            with self.output().open('w') as fp:

                for i in xrange(c):
                    fp.write(actual)
            took = time() - start
            print "took", took
            print float(c) / took

    class B(Task):
        def requires(self):
            return A()

        def run(self):
            with self.input().open('r') as fp:
                for record in fp:
                    pass
                print record

    import os
    try:
        os.unlink("/tmp/blaat")
    except:
        pass

    b = B()
    build([b], local_scheduler=True)
