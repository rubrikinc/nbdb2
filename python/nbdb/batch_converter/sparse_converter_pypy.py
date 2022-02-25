"""
SparseConverter for executing in pypy
"""
from typing import List

from absl import app, flags
from nbdb.batch_converter.sparse_converter import SparseConverter
from nbdb.common.data_point import MODE_BOTH, MODE_REALTIME, MODE_ROLLUP
from nbdb.common.entry_point import EntryPoint
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema

FLAGS = flags.FLAGS
flags.DEFINE_string('cluster_id', '', 'cluster id')
flags.DEFINE_string('batch_json_path', '', 'path for batch json file' )
flags.DEFINE_string('output_dir_path', 'Some default string', 'output dir ppath')
flags.DEFINE_string('batch_filter_file', None,
                    'Batch filter rules file path')
flags.DEFINE_integer('bundle_creation_time', None,
                     'Seconds since epoch when bundle was created')
flags.DEFINE_boolean('compress', True, 'compress as a string')
flags.DEFINE_boolean('batch_mode', False,
                    'Select if schema is loaded in batch mode or not')
flags.DEFINE_enum("consumer_mode", default=MODE_BOTH,
                  enum_values=[MODE_REALTIME, MODE_ROLLUP, MODE_BOTH],
                  help="Consumer mode to run")


class SparseConverterPypy(EntryPoint):
    """
    Entry point class for sparse converter in pypy
    """
    def load_schema(self, schema_mapping: List) -> Schema:
        # We are only loading the first schema file for batch ingestion write
        # path
        _, schema_file = schema_mapping[0].split(":")
        return Schema.load_from_file(schema_file, batch_mode=True)

    @staticmethod
    def no_op_callback(duration_secs: float, points: int) -> None:
        """No op callback"""
        _ = duration_secs
        _ = points

    def start(self):
        sparse_converter = SparseConverter(self.context, Settings.inst,
                                           min_storage_interval=60)

        # Only the manifest file is needed after conversion which is
        # obtained via a static fn of FileBackedSparseStore
        # TODO JARVIS-2708: Print Telemetry to a file so the
        #  dense_batch_consumer reports them. Also remove no_op_callback
        sparse_converter.convert(
            cluster_id=FLAGS.cluster_id,
            batch_json_path=FLAGS.batch_json_path,
            output_dir_path=FLAGS.output_dir_path,
            batch_filter_file=FLAGS.batch_filter_file,
            bundle_creation_time=FLAGS.bundle_creation_time,
            compress=FLAGS.compress,
            consumer_mode=FLAGS.consumer_mode,
            progress_callback=SparseConverterPypy.no_op_callback
        )


if __name__ == '__main__':
    app.run(SparseConverterPypy().run)
