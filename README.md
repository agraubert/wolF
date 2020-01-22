wolF is a cloud-native, Python-based workflow manager optimized for extreme 
scalability (**millions of jobs**).



## Completely Python-based

wolF is not a Domain Specific Language like CWL/WDL, SnakeMake, NextFlow, etc.
This makes it:

  * **Interactive**. Workflows defined and executed in iPython/Jupyter notebooks are
    easy to develop and debug, and their results are seamlessly available for 
    interactive analysis <!-- show example of interactive code -->
  * **Extensible**. If a language feature is not present in a DSL, you're out of 
    luck. With wolF, it's easy to arbitrarily extend workflow functionality with the 
    entirety of the Python ecosystem.

## Cloud and container native

wolF dispatches jobs to an automatically scaling Slurm cluster <!-- be sure to mention seamless handling of Docker somewhere in here -->

# Installation

Currently, wolF is only compatible with Google Cloud Platform, and is meant to be 
installed and run on GCP virtual machines. In the near future, we hope to support 
other cloud platforms. Thus, **the following instructions assume you are running on a 
Google VM running Ubuntu or Debian.**

### Prerequisites

  1. Ensure you have properly authenticated to Google Cloud. Run, following the 
     interactive prompts onscreen,
  
     ```
     gcloud auth login
     ```
     and
  
     ```
     gcloud auth application-default login
     ```
  
     if you have not already.
     
  2. Ensure that Docker is installed on your VM, and that your current user is 
     capable of running Dockers without root (`sudo adduser <USERNAME> docker`)

### Installing

  1. Clone this repository
  
     ```
     git clone --recurse-submodules https://github.com/getzlab/wolF
     ```
     
  2. Run the setup script
     
     ```
     cd wolF/slurm_gcp_docker/src
     ./setup.sh
     ```
     
     This will build a VM image that will serve as a template for cluster worker 
     nodes.

# Quick start