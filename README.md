wolF is a cloud-native, Python-based workflow manager optimized for simplicity and extreme 
scalability (**millions of jobs**).

# Features

**… or, why does the world need yet another workflow manager?** 

### Completely Python-based

wolF is not a Domain Specific Language like CWL/WDL, SnakeMake, NextFlow, etc.
This makes it:

  * **Interactive**. Being able to define and executute wolF workflows in iPython/Jupyter 
    notebooks not only makes workflow development and debugging easy, but by seamlessly
    integrating into your analysis notebook, also makes results instantly available for 
    downstream interactive analysis. Hand wolF a dataframe of inputs, get back a 
    dataframe of your workflow's results. <!-- show example of interactive code -->
  * **Extensible**. If a language feature is not present in a DSL, you're out of 
    luck. With wolF, it's easy to arbitrarily extend workflow functionality with the 
    entirety of the Python ecosystem.

### Cloud and container native

wolF dispatches jobs to an automatically scaling Slurm cluster <!-- be sure to mention seamless handling of Docker somewhere in here -->

### Portable

### Performant

# Installation

wolF will work with any Slurm cluster. For convenient cloud use, wolF comes with a turnkey Slurm installation, which out of the box deploys elastically scaling Slurm clusters across cloud computing VMs.

If you already have a working Slurm cluster, skip ahead to [Standalone Slurm cluster](#install_standalone).

## Google Cloud Platform <a name="install_gcp"></a>

Currently, the only cloud provider compatible with wolF's turnkey solution is Google Cloud Platform. In the near future, we hope to support other cloud platforms. **Thus, the following instructions assume you are running on a Google VM running Ubuntu or Debian.**

### Prerequisites

  1. Ensure you have properly authenticated to Google Cloud. If you have not already, run
     the following commands, following the interactive prompts onscreen:
  
     ```
     gcloud auth login
     ```
     and
  
     ```
     gcloud auth application-default login
     ```
     
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
     
  3. Follow the instructions below for [installing via pip](#install_via_pip).
  
## Standalone Slurm cluster <a name="install_standalone"></a>

If you have a preexisting Slurm cluster (e.g., an on-prem HPC cluster, or a cloud-based
cluster you configured yourself),

     
 1. Install via pip <a name="install_via_pip"></a>

    ```
    pip install wolf-flow
    ```
    
    or, if you want the absolute latest development release, `cd` to the root
    directory of this repository and run
    
    ```
    git checkout develop
    pip install .
    ```

# Quick start