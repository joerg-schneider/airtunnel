
|Build Status| |Code Style: Black| |Python Version|

**Airtunnel** is a means of supplementing `Apache Airflow`_, a platform for
workflow automation in Python which is angled at analytics/data
pipelining. It was born out of years of project experience in data
science, and the hardships of running large data platforms in real life
businesses. Hence, Airtunnel is both a set of principles (read more on
them in the `Airtunnel introduction article`_) and a lightweight Python
library *to tame your airflow*!

Why choose airtunnel?
---------------------

**Because you will…**

❤️ …stop worrying and love the uncompromised consistency

🚀 …need a clean codebase with separated concerns to be scalable

📝 …get metadata for ingested files, load status and lineage
out-of-the-box

🏃 …have it up and running in minutes

🍺 …spend less time debugging Airflow DAGs doing worthwhile things
instead

Getting started
---------------
To get started, we warmly recommended to read the `Airtunnel introduction article`_ and the `Airtunnel tutorial`_.
Also check out the `demo project`_.

Installation
------------

1) We suppose you have installed Apache Airflow in some kind of Python virtual
   environment. From there, simply do a ``pip install airtunnel`` to get
   the package.

2) Configure your codebase according to the Airtunnel principles: You
   need to add three folders for a declaration store, a scripts store
   and finally the data store:

   2.1) The *declaration store* folder has no subfolders. It is where your
   data asset declarations (YAML files) will reside

   2.2) The *scripts store* folder is where all your Python & SQL scripts to process data assets will reside.
   It should be broken down by subfolders ``py`` for Python scripts and ``sql`` for SQL scripts. Please further add
   subfolders ``dml`` and ``ddl`` into the ``sql`` script folder.

   2.3) The *data store* folder follows a convention as well, `refer to the docs`_ on how to structure it.

3) Configure Airtunnel by extending your existing ``airflow.cfg`` (`as documented here`_):

   3.1) Add the configuration section ``[airtunnel]`` in which,
   you need to add three configuration keys.

   3.2) add ``declarations_folder`` which takes the absolute path to the folder you set up in 2.1

   3.3) add ``scripts_folder`` which takes the absolute path to the folder you set up in 2.2

   3.4) add ``data_store_folder``, which takes the absolute path to the folder you set up in 2.3
   for your data store

Installation requirements
~~~~~~~~~~~~~~~~~~~~~~~~~

-  **Python >= 3.6**, **Airflow >=1.10** and **Pandas >= 0.23**

   We assume Airtunnel is implemented best early on in a project, which is why going with a
   recent Python and Airflow version makes the most sense. In the future
   we might do more tests and include coverage for older Airflow
   versions.

-  **PySpark** is supported from **2.3+**

Documentation
-------------
Airtunnel's documentation is `on GitHub pages`_.

.. _Apache Airflow: https://github.com/apache/airflow
.. _on GitHub pages: https://joerg-schneider.github.io/airtunnel/
.. _Airtunnel introduction article: https://medium.com/@schneider.joerg
.. _Airtunnel tutorial: https://joerg-schneider.github.io/airtunnel/tutorial.html
.. _demo project: https://github.com/joerg-schneider/airtunnel-demo
.. _refer to the docs: https://joerg-schneider.github.io/airtunnel/data-store.html
.. _as documented here: https://joerg-schneider.github.io/airtunnel/configuration.html

.. |Build Status| image:: https://dev.azure.com/joerg4805/Airtunnel/_apis/build/status/joerg-schneider.airtunnel?branchName=master
   :target: https://dev.azure.com/joerg4805/Airtunnel/_build/latest?definitionId=1&branchName=master
.. |Code Style: Black| image:: https://img.shields.io/badge/code%20style-black-black.svg
   :target: https://github.com/ambv/black
.. |Python Version| image:: https://img.shields.io/badge/python-3.6%20%7C%203.7-blue.svg
   :target: https://pypi.org/project/airtunnel/