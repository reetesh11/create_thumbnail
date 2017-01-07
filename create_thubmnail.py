import boto
import time
import psycopg2
from PIL import Image
from StringIO import StringIO
#from apscheduler.schedulers.blocking import BlockingScheduler


AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_STORAGE_BUCKET_NAME = 'travelpic-1106'
DBNAME = os.environ.get('DBNAME')
HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')

THUMBNAIL_SIZE = 50, 50

class Database(object):

    def __init__(self):
        self.db_connection = None
        self.cursor = None

    def connect(self):
        try:
            self.db_connection = psycopg2.connect(host=HOST, port=PORT, dbname=DBNAME,
                                                  user=USER, password=PASSWORD)
        except:
            print "Not able to connect to db"
        return self.db_connection

    def get_cursor(self):
        try:
            self.cursor = self.db_connection.cursor()
        except:
            try:
                self.connect()
                self.cursor = self.db_connection.cursor()
            except:
                print "Connection is not established"
        return self.cursor

    def close(self):
        self.cursor.close()
        self.db_connection.close()

class CreateThumbnail(object):
    UPDATE_QUERY = """
                  UPDATE photo 
                  SET thumbnail=%(thumbnail)s 
                  WHERE name = %(name)s
                  """
    
    def __init__(self):
        self.bucket = None
        self.photo_folder = "photo"
        self.thumbnail_folder = "thumbnail"

    def connect_bucket(self):
        conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        self.bucket = conn.get_bucket(AWS_STORAGE_BUCKET_NAME)
        print "Connected to AWS bucket"
        return self.bucket

    def get_folder_list(self, bucket, folder):
        try:
            image_list = bucket.list(folder+"/", "/")
        except:
            image_list = []
        return image_list

    def get_new_images(self):
        photos = self.get_folder_list(self.bucket, self.photo_folder)
        thumbnails = self.get_folder_list(self.bucket, self.thumbnail_folder)
        return photos, thumbnails

    def get_folder_and_file_name(self, filename):
        split = filename.split("/")
        folder, filename = split[0], split[1]
        return str(folder), str(filename)

    def check_difference(self, photos, thumbnail):
        if not photos:
            return
        diff_list = []
        thumbnail_list = [self.get_folder_and_file_name(key.name)[1] for key in thumbnail]
        for key in photos:
            _, filename = self.get_folder_and_file_name(key.name)
            if filename not in thumbnail_list:
                diff_list.append(key)
        return diff_list

    def get_photo_name(self, filename):
        name = filename.split(".")[0]
        return name

    def check_photo_in_database(self, db_cursor, name):
        db_cursor.execute("SELECT name FROM photo WHERE name = '%s'"  %(name))
        row = db_cursor.fetchall()
        if row:
            return True
        else:
            return False

    def update_thumbnail_column(self, db, conn, db_cursor, name, thumbnail):
        try:
            print "Trying to update in db"
            db_cursor.execute("UPDATE photo SET thumbnail=(%s) WHERE name=(%s)",
                              (psycopg2.Binary(thumbnail.getvalue()), name))
            conn.commit()
        except:
            print "Update failed"

    def update_thumbnail_aws_folder(self, filename, thumbnail):
        k = self.bucket.new_key(self.thumbnail_folder+"/"+ filename)
        k.set_contents_from_string(thumbnail.getvalue())
        print "Thumbnail created for %s " %filename

    def update_thumbnail(self, db, conn, db_cursor, name, thumbnail):
        _, filename = self.get_folder_and_file_name(name)
        if self.check_photo_in_database(db_cursor, name):
            self.update_thumbnail_column(db, conn, db_cursor, name, thumbnail)
        else:
            self.update_thumbnail_aws_folder(filename, thumbnail)

    def create_thumbnail(self, new_photos):
        db = Database()
        conn = db.connect()
        for key in new_photos:
            db_cursor = db.get_cursor()
            image_string = key.get_contents_as_string()
            image = Image.open(StringIO(image_string))
            image.thumbnail(THUMBNAIL_SIZE)
            thumbnail = StringIO()
            image.save(thumbnail, image.format)
            self.update_thumbnail(db, conn, db_cursor, key.name, thumbnail)
            #print "Failed to create thumbnail"
    def perform_thumbnail_process(self):
        ph, th = self.get_new_images()
        diff_list = self.check_difference(ph, th)
        self.create_thumbnail(diff_list)

    def perform(self, count=0):
        self.connect_bucket()
        try:
            #while True:
            print "Start Watching"
            self.perform_thumbnail_process()
            print "Stop Watching "
                #time.sleep(100)
        except:
            if count < 3:
                self.perform(count=count+1)
            else:
                print "There is some problem look into it"

def timed_job():
    ct = CreateThumbnail()
    ct.perform()
    #print('This job is run every three minutes.')

#sched.start()
if __name__ =="__main__":
    ct = CreateThumbnail()
    ct.perform()
