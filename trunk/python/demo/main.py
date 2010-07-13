#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A simple guestbook app that demonstrates using the MapReduce libraries."""



from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from mapreduce.operation import db as db_op


# These are defined at the bottom.
HEADER = FOOTER = None


class Post(db.Model):
  name = db.StringProperty(default="")
  message = db.TextProperty(default="")
  time = db.DateTimeProperty(auto_now_add=True)


def lower_case_posts(entity):
  entity.message = entity.message.lower()
  yield db_op.Put(entity)


def upper_case_posts(entity):
  entity.message = entity.message.upper()
  yield db_op.Put(entity)


class Guestbook(webapp.RequestHandler):

  def post(self):
    name = self.request.get("name")
    if name:
      message = self.request.get("message", default_value="")
      Post(name=name, message=message).put()
    self.redirect('/')

  def get(self):
    self.response.out.write(HEADER)
    for post in Post.all().order("-time"):
      self.response.out.write("""
      <p class="signature"> %s
      <br />
      <i>&nbsp;&nbsp;-%s, %s</i></p>
      """ % (post.message, post.name, post.time))
    self.response.out.write(FOOTER)


class DoneHandler(webapp.RequestHandler):

  def post(self):
    pass


def main():
  application = webapp.WSGIApplication([('/', Guestbook),
                                        ('/done', DoneHandler)])
  util.run_wsgi_app(application)


HEADER = """<!DOCTYPE html>
<html>
<head>
<title>Simple Mapreduce Demo</title>
<style type="text/css">
body {
  width: 500px;
  margin: 15px;
}

p.signature, div.form {
  padding: 10px;
  border: 1px solid Navy;
  width: auto;
}

p.signature { background-color: Ivory; }
div.form { background-color: LightSteelBlue; }
</style>
</head>

<body>
<div id="main">
<div id="body">

<h1>Guestbook</h1>

<p style="font-style: italic">
View the <a href="/mapreduce/status">MapReduce status page</a>
to test running jobs.
</p>
<hr />
<div class="form">
Sign the guestbook!

<form action="/" method="POST">
<table>
<tr><td> Name: </td><td><input type="text" name="name"</td></tr>
<td> Message: </td><td><input type="textarea" name="message"</td></tr>
<td /><td><input type="submit" value="Sign"></td>
</table>
</form>
</div>
"""

FOOTER = """
</div></div>
</body>
</html>
"""


if __name__ == '__main__':
  main()
