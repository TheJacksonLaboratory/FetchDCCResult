## DCC API Client

<br>

This project is intend to monitor the staus of files and data that are submitted to DCC. It can pull back feedback of submitted xml files; data of submitted procedure (including specimen, lines etc); and media files. As of today, all IMPC part and EBI procedure part has been implemented and well tested. EBI images needs some more work.


<br>

## Table of Contents 

If your README is long, add a table of contents to make it easy for users to find what they need.

- [Installation](#installation)
- [Usage](#usage)
- [Credits](#credits)

<br>

## Installation

First, `cd` to your work directory, then creates a virtual environment in `Python 3.9+`, if you are like me using  `venv`, use the following command:

```
 python3 -m venv .env/venv_name
```
. Or you can use `conda` or `pyenv`, totally up to you. Next, activate your virtual environment use the following command:

```
. .env/venv_name/bin/activate
```
. In your work directory, use the following command to download the repository:

```
git clone https://github.com/TheJacksonLaboratory/FetchDCCResult.git
```
. Now, you need to install the dependencies of the application, to do that, use the following command:

```
pip install -r requirements.txt
```

<br>

## Usage

Using application is quite straightfoward, but before you use it, please make sure that you have enviormrnt setup for `bash/shell`. If you are on a Mac/Linux machine like me, you are fine. If you are on a windows machine, please use this link to have your enviroment setup. After everything is configured, run the following commands on your terminal:

    ```
    chmod +x /path/to/your/directory/jobs.sh
    /path/to/your/directory/jobs.sh
    ```
`jobs.sh` will execute all tasks for you. 

<br>

## Credits

List your collaborators, if any, with links to their GitHub profiles.

If you used any third-party assets that require attribution, list the creators with links to their primary web presence in this section.

If you followed tutorials, include links to those here as well.

