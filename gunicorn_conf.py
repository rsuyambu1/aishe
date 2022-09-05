from multiprocessing import cpu_count

# Socket Path
bind = 'unix:/var/www/html/WEBANDAPP/aishetrade/gunicorn.sock'

# Worker Options
workers = cpu_count() + 1
worker_class = 'uvicorn.workers.UvicornWorker'

# Logging Options
loglevel = 'debug'
accesslog = '/var/www/html/WEBANDAPP/aishetrade/access_log'
errorlog =  '/var/www/html/WEBANDAPP/aishetrade/error_log'