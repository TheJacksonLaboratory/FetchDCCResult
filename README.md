## DCC API Client

<br>

This project is intend to monitor the staus of files and data that are submitted to DCC. It can pull back feedback of submitted xml files; data of submitted procedure (including specimen, lines etc); and media files. As of today, all IMPC part and EBI procedure part has been implemented and well tested. EBI images needs some more work.


<br>

## Table of Contents 

If your README is long, add a table of contents to make it easy for users to find what they need.

- [Prerequisite](#Prerequisite)
- [Installation](#installation)
- [Usage](#usage)


<br>


## Prerequisite
- Python 3.x 
- Git
- pip3

<br>

## Installation

First, open your `terminal/command prompt` and `cd` to your work directory, then create a virtual environment in `Python 3.8+`. Use the following command if you like to use `venv` just like me:

```
 python3 -m venv .env/venv_name
```
. You can also use `conda` or `pyenv`, whatever you like. Next, activate your virtual environment use the following command:

```
. .env/venv_name/bin/activate
```
. In your work directory, run the following command to download the repository:

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
    chmod +x /path/to/your/directory/impc.sh
    ./path/to/your/directory/impc.sh
    ```
`impc.sh` will execute all tasks for you. If you are on a windows machine, open your command prompt and run the following command:

    ```
    /path/to/your/directory/impc.bat
    ```

<br>

Replace name of Bash\Batchfile script from "impc" to "ebi" if you would like to get feedback of submitted procedures from EBI. Currently, this app is scheduled to run weekly on `bhwin0236.jax.org`. 


