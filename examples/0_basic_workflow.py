# # Toy Workflows
# This notebook showcases the basic functionality of wolF: defining tasks, stringing them together in workflows, and launching container images.
#
# wolF is a minimalistic package: it is entirely contained in the following `import` statement:

from wolf import Task, Workflow, output_helpers

# ## Toy Workflow 1: a single task
#
# Our first workflow will comprise a single task, which takes arrays of numbers and letters, concatenates them with a user-specifiable string, and writes them to a file.

# Workflows are defined as classes, which are derived from wolF's Workflow class.
class Example(Workflow):
    
    # The actual workflow is defined by the workflow() function inside this class.
    # All parameters passed to the task (here, parameter_1) are specified in the function definition.
    def workflow(self, parameter_1):
        
        # Tasks are defined as instances of wolF's task class.
        self.task_A = Task(
            
          # Each task must have a unique name, which is used to identify the task in the output dataframe
          name = "print_the_string",

          # Input parameters are specified as a dict of arrays or scalars. One job will be dispatched for each array element; scalars will be common to all jobs.
          # The following inputs will launch six jobs, with the following parameters:
          #   1. (number = 0, letter = "a", p1 = parameter_1)
          #   2. (number = 1, letter = "b", p1 = parameter_1)
          #   ...
          #   6. (number = 5, letter = "ghi", p1 = parameter_1)
          inputs = {
            "number" : [0,   1,   2,   3,   4,   5],
            "letter" : ["a", "b", "c", "d", "ef", "ghi"],
            "p1" : parameter_1 
          },
            
          # Scripts are defined as an array of bash commands. 
          # Here, we simply concatenate number/p1/letter and write to output.txt
          # We also write p1 to output.abc
          script = [
            "echo -n ${number}_${p1}_${letter} > output.txt",
            "echo -n ${p1} > output.abc"
          ],
            
          # Output entities are inferred from the filenames of files written by the script.
          # They can be specified either as strings or wildcards
          outputs = {
            "number+p1+letter" : "output.txt", # number+p1+letter will only point to a file called "output.txt"
            "p1_only" : "*.abc"                # p1_only will point to any file ending in .abc
          }
        )


# Now that we have defined our workflow, it's time to run it. Workflows are run in a context manager, which will automatically handle spinning up the cluster backend before the workflow starts and tearing it down after the workflow finishes.

with Example(conf = { "compute_script" : "/usr/local/share/cga_pipeline/src/provision_worker_container_host.sh" }) as e:
    e.run(parameter_1 = "foo", run_name = "foo_flow")
    e.run(parameter_1 = "bar", run_name = "bar_flow")
    e.run(parameter_1 = "baz", run_name = "baz_flow")

# By repeatedly invoking the `run()` method of our `Example` workflow class, we dispatch multiple workflows to the same cluster, each with its own value for `parameter_1` as defined in `workflow()` above.
#
# `run_name` is a special parameter that uniquely identifies each workflow in the output dataframe. Once our workflow has finished, we can examine its results dataframe:

e.results

# Results are hierarchically indexed by:
#
#   1. **Workflow run** (`workflow_run_name`): `foo_flow`, `bar_flow`, and `baz_flow`
#   2. **Task name** (`task`): `print_the_string`
#   3. **The array index** within the task (`shard`): 0-5.
#
# Columns are hierarchically organized into three categories:
#
#   1. **Job status** (`job`): exit state, exit code, CPU time
#   2. **Inputs** (`inputs`): `letter`, `number`, `p1`
#   3. **Outputs** (`outputs`): `number+p1+letter`, `p1_only`
#   
# For example, let's look just at `foo_flow`'s outputs:

# +
import pandas as pd
pd.set_option('display.max_colwidth', -1)

e.results.loc["foo_flow", "outputs"]
# -

# Here are just the inputs for shard 5 of every workflow

e.results.loc[(slice(None), slice(None), "5"), "inputs"]


class Example(Workflow):
    def workflow(self, parameter_1, run_task_C = True):
        self.task_A = Task(
          name = "print_the_string",
          inputs = {
            "number" : [0,   1,   2,   3,   42,   1729],
            "letter" : ["a", "b", "c", "d", "ef", "ghi"],
            "p1" : parameter_1 
          },
          outputs = {
            "number+p1+letter" : "*.txt"
          },
          script = [
            "echo -n ${number}_${p1}_${letter} > output.txt",
          ]
        )

        self.task_B = Task(
          name = "get_the_number",
          inputs = {
            "input_file" : self.task_A.get_output("number+p1+letter")
          },
          outputs = {
            "number" : ("*.txt", output_helpers.read_file)
          },
          script = [
            "grep -oE '^[0-9]+' ${input_file} > output.txt"
          ],
          dependencies = self.task_A
        )

        if run_task_C:
            self.task_C = Task(
              name = "get_the_letter",
              inputs = {
                "input_file" : self.task_A.get_output("number+p1+letter")
              },
              outputs = {
                "letter" : ("*.txt", output_helpers.read_file)
              },
              script = [
                "grep -oE '[a-z]+$' ${input_file} > output.txt"
              ],
              dependencies = self.task_A
            )
        else:
            self.task_C = None

        self.task_D = Task(
          name = "concat_results",
          inputs = {
            "input_number" : self.task_B.get_output("number"),
            "input_letter" : self.task_C.get_output("letter") if run_task_C else "z",
          },
          outputs = {
            "final_output" : ("*.txt", output_helpers.read_file)
          },
          script = [
            "echo -n ${input_number},${input_letter} > ${input_number}.txt"
          ],
          dependencies = [self.task_B, self.task_C]
        )

        self.task_E = Task(
          name = "docker_demo",
          inputs = {
            "dummy" : 0
          },
          outputs = {
            "OS" : "*.txt"
          },
          script = [
            "cat /etc/os-release > release.txt"
          ],
          docker = "debian"
        )

with Example() as e:
    e.run(parameter_1 = "foo", run_name = "foo_flow")
    e.run(parameter_1 = "bar", run_task_C = False, run_name = "bar_flow")
    e.run(parameter_1 = "baz", run_task_C = True, run_name = "baz_flow")

e.results
