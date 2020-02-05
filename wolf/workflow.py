from . import Task

import canine

import abc
import os
import pandas as pd

class Workflow:
	def __init__(self, backend = None, conf = {}):
		# add backend
		if backend is None:
			# TODO: this should be a package-wide get_default_backend() method.
			#       that way, tasks can use this too.
			self.backend = canine.backends.dockerTransient.DockerTransientImageSlurmBackend(**{**{
			  "compute_script_file" : None,
			  "image" : None,
			  "cluster_name" : "wolf",
			  "action_on_stop" : "delete",
			  "nfs_action_on_stop" : "run",
			  "compute_zone" : "us-east1-d"
			}, **conf})
		else:
			self.backend = backend

		# all workflows dispatched by this workflow definition
		self.flow_list = {}

		# number of workflows that have been dispatched by this definition
		self.run_index = 0

	@abc.abstractmethod
	def workflow(self, **kwargs):
		pass

	def run(self, run_name = None, **kwargs):
		#
		# copy this workflow
		flow = self.__class__(backend = self.backend)

		#
		# instantiate task objects in copy
		try:
			flow.workflow(**kwargs)
		except:
			print("Workflow is invalid!")
			raise

		if run_name is None:
			run_name = self.__class__.__name__ + "_" + str(self.run_index)
		self.run_index += 1

		#
		# index task objects
		self.flow_list[run_name] = flow._index_tasks()

		#
		# dispatch tasks, with outputs in run-specific directory
		for t in self.flow_list[run_name].values():
			# update output directory of this task to be within parent folder
			# of workflow
			t.set_output_directory(os.path.join(str(run_name), t.output_dir))

			# set workflow name (this is just so that Docker container IDs are
			# unique)
			# TODO: add a check that multiple workflows with the same name
			#       can't be dispatched simultaneously
			t.conf["workflow"] = run_name

			t.run()

	def _index_tasks(self):
		task_index = {}
		for member in self.__dict__.values():
			if isinstance(member, Task):
				# add backend to task if not present
				if member.backend is None:
					member.backend = self.backend
				
				task_index[member.conf["name"]] = member

		return task_index

	def __enter__(self):
		self.backend.__enter__()
		return self

	def __exit__(self, *args):
		try:
			#
			# loop over all workflows dispatched
			w = []
			for workflow in self.flow_list.values():
				#
				# wait for all tasks within this workflow to finish
				for task in workflow.values():
					task.lock.wait()

				# TODO: move this to finally block, so we can always have
				#       at least some results

				#
				# aggregate tasks' results
				r = []
				for task in workflow.values():
					if not task.results.empty:
						r.append(task.results)
					else: # this job was a total failure
						r.append(pd.DataFrame(index = [float('nan')]))

				w.append(pd.concat(r, keys = workflow.keys(), names = ["task", "shard"]))

			# XXX: we might need to put this in a try/catch block -- there are
			#      many more ways that this can get screwed up besides zero output.
			self.results = pd.concat(w, keys = self.flow_list.keys(), names = ["workflow_run_name"]) if len(w) > 0 else None

			# TODO: save results
		except KeyboardInterrupt:
			# TODO: can we register SIGTERM to raise a keyboard interrupt so this
			#       exception can behave identically?
			print("\nWorkflow cancelled by user!")

			#
			# cancel all tasks
			for workflow in self.flow_list.values():
				for task in workflow.values():
					task.cancel()
		finally:
			print("Tearing down cluster, please wait ...")
			self.backend.__exit__()
