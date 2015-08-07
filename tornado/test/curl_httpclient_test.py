from tornado.httpclient import HTTPRequest
from tornado.stack_context import ExceptionStackContext
from tornado.test.httpclient_test import HTTPClientCommonTestCase
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application, RequestHandler
import json

try:
    import pycurl
except ImportError:
    pycurl = None

if pycurl is not None:
    from tornado.curl_httpclient import CurlAsyncHTTPClient

class CurlHTTPClientCommonTestCase(HTTPClientCommonTestCase):
    def get_http_client(self):
        client = CurlAsyncHTTPClient(io_loop=self.io_loop)
        # make sure AsyncHTTPClient magic doesn't give us the wrong class
        self.assertTrue(isinstance(client, CurlAsyncHTTPClient))
        return client

class ContactListHandler(RequestHandler):
    contact_list = {
        "someone": {
            "telephone": "1-800-247-9792",
            "email": "some@email.com"
        },
        "another": {
            "telephone": "1-104-132-7713",
            "email": "other@email.com"
        }
    }

    def get(self, contact_name):
        data = self.contact_list.get(contact_name)
        self.write(data)

    def post(self, contact_name):
        post_data = json.loads(self.request.body)
        for post_item in post_data:
            if post_item["op"] == "replace":
                attribute = post_item["path"]
                value = post_item["value"]
                self.contact_list[contact_name][attribute] = value

    def patch(self, contact_name):
        """
        Patch implementation according to RFC-6902
        http://tools.ietf.org/html/rfc6902
        """
        patch_data = json.loads(self.request.body)
        for patch_item in patch_data:
            if patch_item["op"] == "replace":
                attribute = patch_item["path"]
                value = patch_item["value"]
                self.contact_list[contact_name][attribute] = value

class NonStandardMethodCurlHTTPClientTestCase(AsyncHTTPTestCase):

    def setUp(self):
        super(NonStandardMethodCurlHTTPClientTestCase, self).setUp()
        self.http_client = CurlAsyncHTTPClient(self.io_loop)

    def get_app(self):
        return Application([
            ('/(?P<contact_name>[\w\-]+)', ContactListHandler),
        ])

    def fetch(self, path, body=None, **kwargs):
        kwargs['url'] = self.get_url(path)
        request = HTTPRequest(**kwargs)
        if body is not None:
            request.body = body
        request.allow_nonstandard_methods = True
        self.http_client.fetch(request, self.stop, method=None)
        return self.wait()

    def test_get(self):
        response = self.fetch("/someone", method='GET')
        self.assertEqual(response.code, 200)
        computed_body = json.loads(response.body)
        expected_body = {
            "telephone": "1-800-247-9792",
            "email": "some@email.com"
        }
        self.assertEqual(computed_body, expected_body)

    def test_post(self):
        post_list = [
            {
                "op": "replace",
                "path": "telephone",
                "value": "55-21-99756-1934"
            }
        ]
        body = json.dumps(post_list)
        response_patch = self.fetch("/someone", method='POST', body=body)
        self.assertEqual(response_patch.code, 200)

        response_get = self.fetch("/someone", method="GET")
        computed_body = json.loads(response_get.body)
        expected_body = {
            "telephone": "55-21-99756-1934",
            "email": "some@email.com"
        }
        self.assertEqual(computed_body, expected_body)

    def test_patch_with_payload(self):
        patch_list = [
            {
                "op": "replace",
                "path": "telephone",
                "value": "55-21-99756-1934"
            }
        ]
        body = json.dumps(patch_list)
        response_patch = self.fetch("/someone", method='PATCH', body=body)
        self.assertEqual(response_patch.code, 200)

        response_get = self.fetch("/someone", method="GET")
        computed_body = json.loads(response_get.body)
        expected_body = {
            "telephone": "55-21-99756-1934",
            "email": "some@email.com"
        }
        self.assertEqual(computed_body, expected_body)


# Remove the base class from our namespace so the unittest module doesn't
# try to run it again.
del HTTPClientCommonTestCase

if pycurl is None:
    del CurlHTTPClientCommonTestCase
