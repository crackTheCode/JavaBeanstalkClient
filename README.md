# BeanstalkClient

This is a Java client for the [beanstalkd](http://kr.github.io/beanstalkd/)
work queue broker. It is a fork of the version by
[jpeffer](https://github.com/jpeffer/JavaBeanstalkClient),
which itself was a fork from the version by
[RTykulsker](https://github.com/RTykulsker/JavaBeanstalkClient).

This version has the following changes:

* Add full Javadoc comments.
* Add build.xml file for Ant.
* Always use block IO so that binary payloads can be used.
* Convert unchecked exceptions to checked exceptions.
* Remove ability to have thread local connections.

I do not push my version to Maven Central.

