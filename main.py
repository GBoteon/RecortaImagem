import os
import urllib.request
import pandas as pd
from rembg.bg import remove
import numpy as np
import io
from PIL import Image
from PIL import ImageFile
from pymatting import cutout
from xlsxwriter import Workbook
import logging
import boto3
from botocore.exceptions import ClientError
import MySQLdb

ImageFile.LOAD_TRUNCATED_IMAGES = True
cont = 0
planilha_almofadas = '300_Almofadas.xlsx'
df = pd.read_excel(planilha_almofadas, sheet_name=0)
imgUrl = df.image
amazon_image = []
title = df.title # criar um for
author_id = df.author_id
format_id = df.format_id
DATA = df.DATA
created_at = df.created_at
updated_at = df.updated_at
position = df.position
status = df.status
product_id_legacy = df.product_id_legacy
nome = df.nome
product_id = []
insert_products = []
insert_price_48 = []
insert_price_47 = []
insert_size_48 = []
insert_size_47 = []
insert_finish = []
insert_category = []


BUCKET_NAME = 'urbanarts-images'
client = boto3.client(
    's3',
    aws_access_key_id='credentials',
    aws_secret_access_key='credentials'
)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = client
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={'ACL': 'public-read'})
    except ClientError as e:
        logging.error(e)
        return False
    return True


def dl_jpg(url, file_path, file_name):
    full_path = file_path + file_name
    urllib.request.urlretrieve(url, full_path)


def files_path(path):
    return [os.path.join(p, file) for p, _, files in os.walk(os.path.abspath(path)) for file in files]


print(len(title))

for x in range(len(imgUrl)):
    url = imgUrl[x]
    image = url.split("/")
    image = image[len(image) - 1]
    dl_jpg(url, 'Obras/', image)
    arquivos = files_path('Obras')
    almofada = str(arquivos[0])
    input_path = almofada
    almofada = almofada.split("\\")
    almofada = almofada[len(almofada) - 1]
    output_path = 'Almofadas\\' + almofada[:-4] + '.png'
    amazon_path = 'https://urbanarts-images.s3.sa-east-1.amazonaws.com/media/uploads/almofadas/' + almofada[
                                                                                                   :-4] + '.png'
    amazon_image.append(amazon_path)
    f = np.fromfile(input_path)
    result = cutout(input_path, "C:\\Users\\eduar\\OneDrive\\Documentos\\GitHub\\RecortaFoto\\trimap.png", output_path)
    os.remove(input_path)

for x in range(len(files_path('Almofadas'))):
    almofadas = files_path('Almofadas')
    almofada = str(almofadas[x])
    input_path = almofada
    almofada = almofada.split("\\")
    almofada = almofada[len(almofada) - 1]
    output_path = 'Almofadas\\' + almofada[:-4] + '.png'

    im = Image.open(input_path)

    left = 91
    top = 73
    right = 704
    bottom = 704

    im1 = im.crop((left, top, right, bottom))

    newsize = (800, 800)
    im1 = im1.resize(newsize)
    im1.save(output_path)

while len(files_path('Almofadas')) != 0:
    almofadas = files_path('Almofadas')
    almofada = str(almofadas[0])
    input_path = almofada
    # almofada = almofada.split("\\")
    # almofada = almofada[len(almofada) - 1]
    # upload_file("Almofadas/{}".format(almofada), BUCKET_NAME,
    #             'media/uploads/almofadas/teste/{}'.format(almofada))
    os.remove(input_path)

db_artlab = MySQLdb.connect(host="142.93.242.39", user="forge", passwd="credentials", db="forge_homolog",
                            charset="utf8")
db_verdade = MySQLdb.connect(host="23.92.28.50", user="uaoffice", passwd="credentials", db="db_averdadeestalafora",
                             charset="utf8")
cursor_artlab = db_artlab.cursor()
cursor_verdade = db_verdade.cursor()

for x in range(len(df)):
    insert_products.append("INSERT INTO products (title, author_id, format_id, DATA, image, created_at, updated_at, POSITION, STATUS) VALUES ('" + str(
            title[x]) + "','" + str(author_id[x]) + "','" + str(format_id[x]) + "','" + str(DATA[x]) + "','" + str(
            amazon_image[x]) + "','" + str(created_at[x]) + "','" + str(updated_at[x]) + "','" + str(
            position[x]) + "','" + str(status[x]) + "');")
    cursor_artlab.execute(insert_products[x])
    db_artlab.commit()

    cursor_artlab.execute("SELECT * FROM products WHERE title LIKE '" + str(title[x]) + "';")
    data = cursor_artlab.fetchall()
    product_id.append(data[0][0])

for x in range(len(df)):
    insert_price_48.append("INSERT INTO prices (product_id, finish_id, size_id, price, created_at, updated_at) VALUES('" +
                       str(product_id[x]) + "', 5, 48, 179.00, now(), now());")
    cursor_artlab.execute(insert_price_48[x])
    db_artlab.commit()
    insert_price_47.append("INSERT INTO prices (product_id, finish_id, size_id, price, created_at, updated_at) VALUES('" +
                       str(product_id[x]) + "', 5, 47, 269.00, now(), now());")
    cursor_artlab.execute(insert_price_47[x])
    db_artlab.commit()
    insert_size_48.append("INSERT INTO product_size VALUES('" + str(product_id[x]) + "', 48); ")
    cursor_artlab.execute(insert_size_48[x])
    db_artlab.commit()
    insert_size_47.append("INSERT INTO product_size VALUES('" + str(product_id[x]) + "', 47); ")
    cursor_artlab.execute(insert_size_47[x])
    db_artlab.commit()
    insert_finish.append("INSERT INTO finish_product VALUES( 5, '" + str(product_id[x]) + "'); ")
    cursor_artlab.execute(insert_finish[x])
    db_artlab.commit()
    cursor_verdade.execute("SELECT * FROM `produtos_images` WHERE nome LIKE '" + str(title[x])[11:] + "'")
    category = cursor_verdade.fetchall()
    insert_category.append(
                "INSERT INTO category_product VALUES('" + str(category[0][7]) + "','" + str(product_id[x]) + "');")
    cursor_artlab.execute(insert_category[x])
    db_artlab.commit()

df1 = pd.DataFrame({'product_id': product_id, 'imagem_amazon': amazon_image, 'insert_product': insert_products, 'insert_price_48': insert_price_48, 'insert_price_47': insert_price_47, 'insert_size_48': insert_size_48, 'insert_size_47': insert_size_47, 'insert_finish': insert_finish, 'insert_category': insert_category})
writer = pd.ExcelWriter('log.xlsx', engine='xlsxwriter')
df1.to_excel(writer, sheet_name='Sheet1', index=False)
writer.close()
