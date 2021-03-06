import logging
import os
from ..cloudobjectbase import CloudObjectBase

import subprocess

logger = logging.getLogger(__name__)


class GZippedBlob(CloudObjectBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self):
        with self.cloud_object.s3.open(self.cloud_object.path, 'rb') as co:
            r, w = os.pipe()

            proc = subprocess.Popen(['/home/lab144/.local/bin/gztool', '-i', '-x', '-s', '10'],
                                    stdin=r)
            pipe = os.fdopen(w, 'wb')

            chunk = co.read(65536)
            while chunk != b"":
                pipe.write(chunk)
                chunk = co.read(65536)

            i_file, stderr = proc.communicate()
            assert stderr == b""

        # TODO
        # add creation of text index file
        r, w = os.pipe()
        proc = subprocess.Popen(['/home/lab144/.local/bin/gztool', '"'+r+'i"', '>', '"'+r+'i.info"'],
                                    stdin=r)
        iinfo_file, stderr = proc.communicate()

        proc = subprocess.Popen(['sed', '-e', "'s/#/\n#/g'", '-e', "'s/L//g'", '"'+r+'i.info"', '|', 'awk', "' /^#[0-9]+/ { printf ('%s %s\n', $3, $6);} '",
        '>', '"'+r+'i_tab.info"'],
                                    stdin=r)
        i_tabinfo_file, stderr = proc.communicate()

        # upload both index file and text index file to object storage
        with self.cloud_object.s3.open(self.cloud_object.path, 'rb') as co:
            co.write(i_file)
            co.write(iinfo_file)
            co.write(i_tabinfo_file)


    def partition_even_lines(self, lines_per_chunk):
        # TODO
        # make use of index to get the ranges of the chunks partitioned by number of lines per chunk
        # return list of range tuples [(chunk0-range0, chunk-0range1), ...]
        pass


class GZippedText(GZippedBlob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
