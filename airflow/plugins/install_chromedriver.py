from webdriver_manager.chrome import ChromeDriverManager
from shutil import copyfile
import os

path = ChromeDriverManager().install()
copyfile(path, '/usr/bin/chromedriver')
os.chmod('/usr/bin/chromedriver', 0o755)
