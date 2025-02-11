# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Script to generate all the files needed for importing the WHO GHO dataset."""

import os
from absl import app
from absl import flags
from generate_schema import generate_schema
from generate_csv_and_sv import generate_csv_and_sv

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'data_dir', '',
    'Path to folder that contains the raw json data from the indicators.')
flags.DEFINE_string(
    'curated_dim_file', '',
    'path to a json file containing mapping of dimension types and values to their schema dcid. (For curated values)'
)
flags.DEFINE_string(
    'artifact_dir', '',
    'dir to output generated artifacts (dimension categories and dimension to schema dcid mapping). If blank, will not output dimension categories'
)
flags.DEFINE_string(
    'output_dir', '',
    'dir to output generated MCF files and csv to. If blank, will output the generated files to current directory'
)


def import_data(data_files, curated_dim_file, output_dir, artifact_dir):
    schema_mapping = generate_schema(data_files, curated_dim_file, artifact_dir,
                                     output_dir)
    generate_csv_and_sv(data_files, schema_mapping, output_dir)


def main(args):
    data_dir = FLAGS.data_dir
    data_files = []
    if FLAGS.data_dir:
        for f in os.listdir(data_dir):
            data_files.append(os.path.join(data_dir, f))
    import_data(data_files, FLAGS.curated_dim_file, FLAGS.output_dir,
                FLAGS.artifact_dir)


if __name__ == '__main__':
    app.run(main)
