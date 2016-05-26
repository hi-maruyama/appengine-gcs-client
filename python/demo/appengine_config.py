# coding: utf-8
"""
`appengine_config.py` is automatically loaded when Google App Engine
starts a new instance of your application. This runs before any
WSGI applications specified in app.yaml are loaded.
"""

import os
from google.appengine.ext import vendor

# Third-party libraries are stored in "lib", vendoring will make
# sure that they are importable by the application.
#vendor.add('lib')
vendor.add(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'lib'))

import os
from google.appengine.api.app_identity import app_identity
def namespace_manager_default_namespace_for_request():
    """ デフォルトのネームスペースを決める
    http://yellow-1317.appspot.com/datastore >> yellow-1317.appspot.com
    http://near109.com/datastore >> near109.com
    """
    return os.environ.get('HTTP_HOST', '').replace(":", "")
