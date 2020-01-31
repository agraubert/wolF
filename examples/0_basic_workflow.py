from wolf import Task, Workflow, output_helpers

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
