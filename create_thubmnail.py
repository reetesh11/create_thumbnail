import boto
import time
from PIL import Image
from StringIO import StringIO

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_STORAGE_BUCKET_NAME = 'travelpic-1106'

THUMBNAIL_SIZE = 200, 150


class CreateThumbnail(object):
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
            
    def create_thumbnail(self, new_photos):
        for key in new_photos:
            try:
                folder, filename = self.get_folder_and_file_name(key.name)
                image_string = key.get_contents_as_string()
                image = Image.open(StringIO(image_string))
                image.thumbnail(THUMBNAIL_SIZE)
                thumbnail = StringIO()
                image.save(thumbnail, image.format)
                k = self.bucket.new_key(self.thumbnail_folder+"/"+ filename)
                k.set_contents_from_string(thumbnail.getvalue())
                #print "Thumbnail created for %s " %filename
            except:
                print "Failed to create thumbnail"
            
    def perform_thumbnail_process(self):
        ph, th = self.get_new_images()
        diff_list = self.check_difference(ph, th)
        self.create_thumbnail(diff_list)
            

    def perform(self, count=0):
        self.connect_bucket()
        try:
            while(True):
                print "Start Watching"
                self.perform_thumbnail_process()
                print "Stop Watching "
                time.sleep(100)
        except:
            if count < 3:
                self.perform(count=count+1)
            else:
                print "There is some problem look into it"
            

if __name__ == "__main__":
    ct = CreateThumbnail()
    ct.perform()
    
    
