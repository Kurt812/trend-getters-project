![Pylint Score](.github/badges/pylint.svg) 
![Passing Tests](.github/badges/test.svg)

# Trend Getter
## A tool that allows users to select topics/tags on key sites (Google Trends, BlueSky) and monitor their growth/interest over time.

<details>
  <summary>Table of Contents 📝</summary>
  <ol>
    <li>
      <a href="#about-the-project-">About The Project</a>
      <ul>
        <li><a href="#diagrams-">Diagrams</a></li>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li>
      <a href="#requirements-">Requirements</a>
    </li>
    <li>
      <a href="#folders-explained-">Folders Explained</a>
    </li>
    <li>
      <a href="files-explained">Files Explained</a>
    </li>
    <li>
      <a href="#developers-">Developers</a>
    </li>
  </ol>
</details>

## About the Project


### Diagrams 📊

#### Architecture Diagram

![Architecture Diagram]()

#### ERD Diagram

![ERD Diagram](/images/ERD-diagram.png)

#### Dashboard Wireframe

![Dashboard Wireframe]()

### Built With
 [![Python][Python.com]][Python-url]


## Getting Started 🛠️

### Prerequisites:
- Python 3.12 must be installed with pip3 for dependency installation.  

### Installation:
These instructions are for **macOS only**.

1. **Clone the repository to your local machine using the following command**:
    ```sh
    git clone https://github.com/Kurt812/trend-getters-project.git
    ```
2. **Navigate into the cloned repository**.
3. **Setup venv environment**:
    ```zsh
    python3.12 -m venv .venv
    source .venv/bin/activate
    ```
4. **Install all required dependencies**:
    ```sh
    pip3 install -r requirements.txt
    ```
5. **Configure AWS CLI**:
    - Install AWS CLI if you don't have it already:
        ```sh
        brew install awscli
        ```
    - Configure your AWS credentials by running:
        ```sh
        aws configure
        ```
      You will be prompted to enter your AWS Access Key ID, Secret Access Key, region, and output format. Make sure to provide your AWS credentials when prompted.

7. **Login to AWS**:
    - Once configured, you can log in to AWS from your terminal using:
        ```sh
        aws sts get-caller-identity
        ```
      This will confirm that you are authenticated and have access to your AWS resources.

## Requirements 📋
You must have a `.env` file containing:   
| Variable         | Description                                      |
|------------------|--------------------------------------------------|
|       |     |


   
## Folders Explained 📁
These folders are found this repository:     
- **[images](https://github.com/Kurt812/trend-getters-project/tree/main/images)**     
   
- **[dashboard](https://github.com/Kurt812/trend-getters-project/tree/main/dashboard)** 

- **[pipeline](https://github.com/Kurt812/trend-getters-project/tree/main/pipeline)**

- **[terraform](https://github.com/Kurt812/trend-getters-project/tree/main/terraform)**  
  


## Files Explained🗂️
These files are found in this repository:
- **README.md**  
  This is the file you are currently reading, containing information about each file.   
- **requirements.txt**  
  This project requires specific Python libraries to run correctly. These dependencies are listed in this file and are needed to ensure your environment matches the project's environment requirements.


[Python.com]: https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54
[Python-url]: https://www.python.org/


## Developers 👨🏽‍💻👩🏽‍💻
This project was developed by the following contributors:

- **[Kurt Martin-Brown](https://github.com/Kurt812)** - Project Manager & Data Engineer
- **[Ridwan Hamid](https://github.com/RidwanHamid501)** - Architect & Data Engineer
- **[Keogh Jokhan](https://github.com/keoghrmj)** - Quality Assurance & Data Engineer
- **[Surina Santhokhy](https://github.com/SurinaCS)** - Quality Assurance & Data Engineer
