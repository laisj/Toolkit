from PIL import ImageGrab
import datetime
import time

timeStr = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
im = ImageGrab.grab()
datetime.time()
im.save(r'd:\\ScreenMonitor\\' + timeStr + '.jpg', 'jpeg')
