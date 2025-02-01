import appdirs
import os
import sys
import geoip2
import geoip2.database
import shutil
import tarfile
import urllib.request as request
from contextlib import closing

global API_KEY
API_KEY = os.environ["MAXMIND_API_KEY"]

URL = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&suffix=tar.gz&license_key='


class GeoliteCity(object):

    def __init__(self) -> None:
        self.folder = appdirs.user_cache_dir('geolite', 'ihr')
        self.dbfname = ''
        self.reader = None

    def download_database(self, overwrite=True):

        url = URL+API_KEY
        os.makedirs(self.folder, exist_ok=True)
        fname = '/geolite_city.tar.gz'

        if not os.path.exists(self.folder+fname) or overwrite:
            sys.stderr.write(f'Downloading: {url}\n')
            with closing(request.urlopen(url)) as r:
                with open(self.folder+fname, 'wb') as f:
                    shutil.copyfileobj(r, f)

        # Extract the tar archive
        if fname.endswith("tar.gz"):
            tar = tarfile.open(self.folder+fname, "r:gz")
            tar.extractall(self.folder)

            # find the database file name
            for tar_fname in tar.getnames():
                if tar_fname.endswith('.mmdb'):
                    self.dbfname = self.folder+'/'+tar_fname

            tar.close()

    def load_database(self):
        self.download_database(overwrite=False)
        self.reader = geoip2.database.Reader(self.dbfname)

    def lookup(self, ip):
        """Find the country code for the given IP address"""
        cc = "ZZ"
        try:
            cc = self.reader.city(ip).country.iso_code
        except geoip2.errors.AddressNotFoundError:
            pass

        if cc is None:
            cc = "ZZ"

        return cc


if __name__ == '__main__':

    gc = GeoliteCity()
    gc.download_database(overwrite=False)
    gc.load_database()

    print(gc.lookup(sys.argv[1]))
