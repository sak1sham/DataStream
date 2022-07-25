# DataStream

This is the python based data migration service (with inbuilt scheduler) which has the ability to migrate from many common sources to one or more of the commonly used destinations.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install before starting to understand and contribute to the project

1. [Python 3.8](https://www.python.org/downloads/release/python-380/)
2. Environment managers like [conda](https://docs.conda.io/en/latest/) are useful, but optional

### Installing

After having the above prerequisites, here is a step by step series of examples that tell you how to get a development env running

1. [pip 21.2.4](https://pypi.org/project/pip/21.2.4/): This project has some dependencies that work best with pip==21.2.4. To get this particular version, run the following command in your terminal

```
pip3 install --upgrade pip==21.2.4
```

2. Getting awscli to interact with the AWS ecosystem

```
pip3 --no-cache-dir install --upgrade awscli
```

Have a look at this [quick-setup guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html) to configure the aws credentials with awscli. This step is optional.

3. Getting [pip-tools](https://github.com/jazzband/pip-tools) for handling all the dependencies for the project.

```
pip3 install pip-tools
```

4. Compile all dependencies from requirements.in file. This will create a new ```requirements.txt``` file containing a beautifully-formatted and understandable dependency tree for the project. 

```
pip-compile
```

5. Once the ```requirements.txt``` file is created, we are ready to install all libraries.

```
pip-sync
```

This command might take a while to run, but once it's complete, you will have the system ready to be run.

6. To test the status, run the following commands:
```
cd src
python main.py __test__
```

## Storing Information of Data-Pipelines

Although the script seems to be starting-up fine, you will need to setup a mongodb collection to run any job. This will be used as a storage for jobs' information.

You can create a ```.env``` file to store these credentials as environment variables (Highly recommended), or can hard-code these inside the files (Not recommended). [settings](src/config/settings.py) file also needs to be modified to be able to access the mongoDB URLs. Refer [this documentation](src/config//README.md) to learn what each field in the settings file corresponds to.

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

To run the script, you need to configure your settings and add the mappings for the required data pipelines inside the [config](src/config/) folder.

Additional notes about how to deploy this on a live system. It's best to run the script inside an isolated container. We can make use of docker. 

For running this script inside a docker container, you need to make sure dockerhub is installed and running on your system locally. Create a new file ```Dockerfile``` in the root directory, and copy the following contents.

```
FROM python:3.8

RUN pip3 install --upgrade pip==21.2.4

RUN pip3 --no-cache-dir install --upgrade awscli &&\
    pip3 install pip-tools

COPY requirements.in .

RUN pip-compile &&\
    pip-sync

COPY ./src /src
WORKDIR "/src"

CMD python main.py my_job_1 my_job_2
```

Here, replace the my_job_1, my_job_2, etc. with the name (.py file name) of your jobs inside src/config/jobs/

Once this dockerfile is created, refer [this](https://docs.docker.com/get-started/02_our_app/) documentation to create an image and run a container using this file. 

In case you want to run the script without any isolated environment, i.e., directly on your local system, you can do so by following the [prerequisites](#prerequisites) and [installation](#installing) instructions. Then, you can directly run the following commands.

```
cd src
python main.py my_job_1 my_job_2
```
Here, replace the my_job_1, my_job_2, etc. with the name (.py file name) of your jobs inside src/config/jobs/

## Built With

* [Python 3](https://www.python.org/downloads/release/python-380/) - The bread-and-butter of this script

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Saksham Garg, Mukesh Bhati, Anchit Tandon** - *Initial work*

See also the list of [contributors](https://github.com/sak1sham/DataStream/graphs/contributors) who participated in this project.

## License

This project is licensed under the GPL-3.0 license - see the [LICENSE.md](LICENSE.md) file for details
