"""
Run sparse converter on batch JSON.
If none is specified, run with prod schema and settings.
"""
import argparse
import logging
import os

from nbdb.batch_converter.sparse_converter import SparseConverter
from nbdb.common.context import Context
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema

logger = logging.getLogger()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )

    parser = argparse.ArgumentParser(
        description='Run sparse converter locally.')
    parser.add_argument('--cluster_id', type=str, required=True)
    parser.add_argument('--batch_json_path', type=str, required=True)
    parser.add_argument('--output_dir_path', type=str, required=True)
    parser.add_argument('--schema_path', type=str, required=False)
    parser.add_argument('--settings_path', type=str, required=False)
    parser.add_argument('--compress', action="store_true")
    args = parser.parse_args()

    if args.schema_path:
        schema_path = args.schema_path
    else:
        schema_path = os.path.join(
                os.path.dirname(__file__), "../..", "config",
                "schema_prod.yaml")

    if args.settings_path:
        settings_path = args.settings_path
    else:
        settings_path = os.path.join(
                os.path.dirname(__file__), "../..", "config",
                "settings_prod.yaml")

    schema = Schema.load_from_file(schema_path, batch_mode=True)
    Settings.load_yaml_settings(settings_path)
    context = Context(schema=schema)

    converter = SparseConverter(
        context=context,
        settings=Settings.inst,
        min_storage_interval=60)
    logger.info("Starting converter with args %s", args)
    converter.convert(
        cluster_id=args.cluster_id,
        batch_json_path=args.batch_json_path,
        output_dir_path=args.output_dir_path,
        compress=args.compress)
