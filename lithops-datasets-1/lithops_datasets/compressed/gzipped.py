import logging
import os
from ..cloudobjectbase import CloudObjectBase
import pathlib
import shutil
import tempfile
from lithops import Storage
import subprocess as sp

logger = logging.getLogger(__name__)

CWD = os.getcwd()
CURRENT_PATH = str(pathlib.Path(__file__).parent.resolve())
LOCAL_TMP = os.path.realpath(tempfile.gettempdir())
INDEXES_PREIX = 'genomics/indexes/'
CHUNKS_PREIX = 'genomics/chunks/'

class AuxiliarFunctions():
    def gzfile_info():
        file = input('Indicate the name of the gz file to chunk:')
        lines = input('Indicate the number of lines to retrieve for a chunk:')
        return file, lines


    # Function to get input information
    def gzfile_info_random():
        file = input('Indicate the name of the gz file to chunk:')
        start_line = input('Indicate the start line of the chunk:')
        end_line = input('Indicate the end line of the chunk:')
        return file, start_line, end_line


    # Function to remove all non-numeric characters from a string
    def only_numerics(string):
        string_type = type(string)
        return string_type().join(filter(string_type.isdigit, string))


    def get_total_lines(file):
        """
        gets the total number of lines from the index_tab.info file
        """
        with open(file+'i_tab.info', 'r') as f:
            for last_line in f:
                pass

        return last_line.split(' ')[1]


    # Function to read information of the chunks from a file
    def read_chunks_info(file):
        chunk_list = []
        counter = 0
        with open(file+'.chunks.info', 'r') as f:
            for line in f:
                print('Chunk info:'+str(counter))
                info = line.split()
                chunk_list.append({'number': (counter+1), 'start_line': str(info[0]),
                                'end_line': str(info[1]), 'start_byte': str(info[2]),
                                'end_byte': str(info[3])})
                counter += 1
        return chunk_list, counter


    # Function to read information of the chunks from a file
    def read_chunk_info_random(file, start_line, end_line):
        with open(file+'.random_chunk_'+start_line+'_'+end_line+'.info', 'r') as f:
            for line in f:
                info = line.split()
                chunk = {'number': (1), 'start_line': str(info[0]), 'end_line': str(info[1]),
                        'start_byte': str(info[2]), 'end_byte': str(info[3])}
        return chunk


class GZippedBlob(CloudObjectBase, AuxiliarFunctions):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, bucket_name, file_name):
        with self.cloud_object.s3.open(self.cloud_object.path, 'rb') as co:
            r, w = os.pipe()

            proc = sp.Popen(['/home/lab144/.local/bin/gztool', '-i', '-x', '-s', '10'],
                                    stdin=r)
            pipe = os.fdopen(w, 'wb')

            chunk = co.read(65536)
            while chunk != b"":
                pipe.write(chunk)
                chunk = co.read(65536)

            stdout, stderr = proc.communicate()
            assert stderr == b""

        # TODO
        # add creation of text index file
        # upload both index file and text index file to object storage

        os.chdir(LOCAL_TMP)
        storage = Storage()

        # 0 DOWNLOAD FASTQ.GZ FILE TO LOCAL TMP
        local_filename = os.path.join(LOCAL_TMP, file_name)
        if not os.path.isfile(local_filename):
            print(f'Downloading cos://{bucket_name}/{file_name} to {LOCAL_TMP}')
            obj_stream = storage.get_object(bucket_name, file_name, stream=True)
            with open(local_filename, 'wb') as fl:
                shutil.copyfileobj(obj_stream, fl)

        # 1 GENERATING THE INDEX AND INFORMATION FILES AND UPLOADING TO THE BUCKET
        sp.run(CURRENT_PATH+'/generateIndexInfo.sh '+local_filename, shell=True, check=True, universal_newlines=True)
        output = sp.getoutput(CURRENT_PATH+'/generateIndexInfo.sh '+local_filename)
        output = output.split()
        total_lines = str(AuxiliarFunctions.only_numerics(output[-3]))

        # 2. UPLOAD FILES TO THE BUCKET
        remote_filename = INDEXES_PREIX+file_name

        print(f'Uploading {local_filename}i to cos://{bucket_name}/{INDEXES_PREIX}')
        with open(f'{local_filename}i', 'rb') as fl:
            storage.put_object(bucket_name, f'{remote_filename}i', fl)

        print(f'Uploading {local_filename}i.info to cos://{bucket_name}/{INDEXES_PREIX}')
        with open(f'{local_filename}i.info', 'rb') as fl:
            storage.put_object(bucket_name, f'{remote_filename}i.info', fl)

        print(f'Uploading {local_filename}i_tab.info to cos://{bucket_name}/{INDEXES_PREIX}')
        with open(f'{local_filename}i_tab.info', 'rb') as fl:
            storage.put_object(bucket_name, f'{remote_filename}i_tab.info', fl)

        os.remove(f'{local_filename}i')
        os.remove(f'{local_filename}i.info')
        os.remove(f'{local_filename}i_tab.info')

        os.chdir(CWD)

    def partition_even_lines(self, AuxiliarFunctions, lines_per_chunk,bucket_name, file_name):
        # TODO
        # make use of index to get the ranges of the chunks partitioned by number of lines per chunk
        # return list of range tuples [(chunk0-range0, chunk-0range1), ...]
        """
        This function creates the partitions of x 'LINES' of the compressed file,
        unzips them and stores them in the bucket.
        """
        storage = Storage()

        # 1 DOWNLOAD INDEX FILES
        """
        Downloads index files of a given fastq file if not present in the local machine
        """
        os.chdir(LOCAL_TMP)

        storage = Storage() if not storage else storage
        print(f'\n--> Downloading {file_name} index files')

        remote_filename = INDEXES_PREIX+file_name

        if not os.path.isfile(f'{file_name}i'):
            remote_filename_i = f'{remote_filename}i'
            obj_stream = storage.get_object(bucket_name, remote_filename_i, stream=True)
            with open(f'{file_name}i', 'wb') as fl:
                shutil.copyfileobj(obj_stream, fl)

        if not os.path.isfile(f'{file_name}i.info'):
            remote_filename_i = f'{remote_filename}i.info'
            obj_stream = storage.get_object(bucket_name, remote_filename_i, stream=True)
            with open(f'{file_name}i.info', 'wb') as fl:
                shutil.copyfileobj(obj_stream, fl)

        if not os.path.isfile(f'{file_name}i_tab.info'):
            remote_filename_i = f'{remote_filename}i_tab.info'
            obj_stream = storage.get_object(bucket_name, remote_filename_i, stream=True)
            with open(f'{file_name}i_tab.info', 'wb') as fl:
                shutil.copyfileobj(obj_stream, fl)

        os.chdir(CWD)

        # 2 GENERATING LINE INTERVAL LIST AND GETTING CHUNK'S BYTE RANGES
        print('\n--> Generating chunks')
        os.chdir(LOCAL_TMP)
        total_lines = AuxiliarFunctions.get_total_lines(file_name)
        block_length = str(lines_per_chunk)
        sp.run(CURRENT_PATH+'/generateChunks.sh '+file_name+' '+block_length+' '+total_lines,
            shell=True, check=True, universal_newlines=True)
        chunks, chunk_counter = AuxiliarFunctions.read_chunks_info(file_name)

        print(chunks)

        # 3 RETRIEVE CHUNKS FROM BUCKET AND UNZIP THEM
        for chunk in chunks:
            print("\n--> Processing chunk {}... ".format(chunk['number']))

            byte_range = f"{int(chunk['start_byte'])-1}-{int(chunk['end_byte'])}"
            obj_stream = storage.get_object(bucket_name, file_name, extra_get_args={'Range': f'bytes={byte_range}'}, stream=True)

            local_chunk_filename = os.path.join(LOCAL_TMP, file_name.replace('.fastq.gz', f'_chunk{chunk["number"]}.fastq'))
            local_chunk_filename_gz = f"{local_chunk_filename}.gz"

            with open(local_chunk_filename_gz, 'wb') as fl:
                shutil.copyfileobj(obj_stream, fl)

            cmd = f'gztool -I {file_name}i -n {chunk["start_byte"]} -L {chunk["start_line"]} {local_chunk_filename_gz} | head -{block_length} > {local_chunk_filename}'
            sp.run(cmd, shell=True, check=True, universal_newlines=True)

            print(f'Uploading {local_chunk_filename} to cos://{bucket_name}/{CHUNKS_PREIX}')
            with open(local_chunk_filename, 'rb') as fl:
                remote_chunk_filename = CHUNKS_PREIX+file_name.replace('.fastq.gz', f'_chunk{chunk["number"]}.fastq')
                storage.put_object(bucket_name, remote_chunk_filename, fl)

            os.remove(local_chunk_filename)
            os.remove(local_chunk_filename_gz)

            tuples = []
            tuples.append({int(chunk['start_byte'])-1,int(chunk['end_byte'])})

        print(str(chunk_counter)+" chunks decompressed.")

        os.chdir(CWD)
        
        return tuples

class GZippedText(GZippedBlob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
