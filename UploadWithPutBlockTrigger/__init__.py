import base64
import time
import logging
import requests
from datetime import datetime
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Define the upload file
    file_to_upload = 'C:\\Users\\2194726\\Downloads\\sample_consolidated.pdf'

    # Define the block size
    chunk_size = 1024 * 1024 * 250  # 250 MB

    blob_service_url = 'https://nrstorageaccount.blob.core.windows.net/'
    container_name = 'blockblobcontainer/'
    blob_name = 'blocks'
    blob_sas_token = '?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2022-09-03T11:18:23Z&st=2022-09-01T02:38:23Z&spr=https&sig=JoYGiFgipVN5lOA5wd4NgnlvWTW7%2FjPe9vANZI6D85w%3D'

    # Create SAS URI
    blob_service_sas_url = blob_service_url + \
        container_name + blob_name + blob_sas_token

    try:

        # Read the file
        with open(file=file_to_upload, mode='rb') as file:
            file_bytes = file.read(chunk_size)
            l = len(file_bytes)
            bytes_id = b'0000'   # Initialize block id
            block_list = []

            while len(file_bytes) > 0:
                # Generate URL-encoded Base64 string
                block_id = base64.urlsafe_b64encode(bytes_id)

                # Create hrequest headers
                headers = create_headers(str(len(file_bytes)))

                # Create Put Block request URI
                put_block_uri = blob_service_sas_url + \
                    f'''&comp=block&blockid={block_id.decode('UTF-8')}'''

                # Make Put Block request
                response = requests.put(
                    url=put_block_uri, data=file_bytes, headers=headers)

                # If response is ok then append block id to block_list variable
                if response.ok:
                    block_list.append(block_id.decode("utf-8"))

                # Convert block id to integer, increase it by 1 and convert it back to bytes
                int_id = int(bytes_id.decode('utf-8'))
                int_id += 1
                # Convert integer to string and pad 4 zeros to left
                str_id = f'{int_id:04}'
                bytes_id = bytes(str_id, 'UTF-8')

                file_bytes = file.read(chunk_size)
                l += len(file_bytes)

        # Create Put BlockList request URI
        put_blocklist_uri = blob_service_sas_url + \
            f'''&comp=blocklist'''

        # Create hrequest headers
        headers = create_headers(str(l))

        # lst = '<?xml version="1.0" encoding="UTF-8"?>'
        lst = '<BlockList>'
        for i in block_list:
            lst += '<Latest>'+str(i)+'</Latest>'
        lst += '</BlockList>'

        # Make Put BlockList request
        response = requests.put(
            url=put_blocklist_uri, data=lst, headers=headers)

        if response.ok:
            return func.HttpResponse(f'{response.content}')

    except Exception as ex:
        return func.HttpResponse(f'{ex}')


def create_headers(content_length):
    """ Create request headers """

    current_date_time = datetime.now()
    gmt_date_time = time.strftime(
        "%a, %d %b %Y %H:%M:%S", current_date_time.timetuple())+' GMT'
    headers = {
        'x-ms-date': gmt_date_time,
        'x-ms-version': '2019-12-12',
        'Content-Length': str(content_length)
    }
    return headers
