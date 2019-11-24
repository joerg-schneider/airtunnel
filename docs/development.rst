Development & testing
=====================

Airtunnel is developed as a Python project on GitHub. We use Azure DevOps for CI/CD, along with tox for automated
testing (using PyTest) and flit for building/publishing.

Airtunnel has been tested rigorously, with a test coverage above 96%. To resemble the real world, our test executes
several actual Airflow DAGs through Airflow from PyTest.

Still, anything might break or not work given different conditions – please do not hesitate to open an issue on Github.