import os
class ReverseProxied(object):
    '''Wrap the application in this middleware and configure the 
    front-end server to add these headers, to let you quietly bind 
    this to a URL other than / and to an HTTP scheme that is 
    different than what is used locally.

    In nginx:
    location /myprefix {
        proxy_pass http://192.168.0.1:5001;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Scheme $scheme;
        proxy_set_header X-Script-Name /myprefix;
        }

    :param app: the WSGI application
    '''
    def __init__(self, app):
        self.app = app
        self.BaseURL = '/' + os.path.dirname(os.path.realpath(__file__)).rsplit('\\', 1)[-1]

    def __call__(self, environ, start_response):
        # script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
        script_name = '/' + os.path.dirname(os.path.realpath(__file__)).rsplit('\\', 1)[-1]
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        return self.app(environ, start_response)


class PrefixMiddleware(object):

    def __init__(self, app, BaseURL=''):
        self.app = app
        self.BaseURL = BaseURL

    def __call__(self, environ, start_response):

        if environ['PATH_INFO'].startswith(self.BaseURL):
            environ['PATH_INFO'] = environ['PATH_INFO'][len(self.BaseURL):]
            environ['SCRIPT_NAME'] = self.BaseURL
            return self.app(environ, start_response)
        else:
            start_response('404', [('Content-Type', 'text/plain')])
            return ["This url does not belong to the app.".encode()]