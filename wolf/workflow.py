from . import Task

import canine

import abc
import os
import pandas as pd
import threading
import warnings

class Workflow(object):
	def __init__(self, backend = None, strategy = None, conf = None):
		if conf is None:
			conf = {}
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

		self.strategy = strategy if strategy is not None else 'NFS'

		# all workflows dispatched by this workflow definition
		self.flow_list = {}
		self.outputs = {}

		# number of workflows that have been dispatched by this definition
		self.run_index = 0

	@abc.abstractmethod
	def workflow(self, **kwargs):
		pass

	def run(self, run_name = None, **kwargs):
		#
		# copy this workflow
		# AG: Why?
		flow = self.__class__(backend = self.backend, strategy=self.strategy)

		#
		# instantiate task objects in copy
		try:
			final_outputs = flow.workflow(**kwargs)
			if final_outputs is None:
				warnings.warn(
					"Workflow delcared no outputs. Output files will not be delocalized"
				)
		except:
			print("Workflow is invalid!")
			raise

		if run_name is None:
			run_name = self.__class__.__name__ + "_" + str(self.run_index)
		self.run_index += 1

		#
		# index task objects
		self.flow_list[run_name] = flow._index_tasks()
		self.outputs[run_name] = (flow, final_outputs)

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

	def delocalize(self, outputs, output_dir = 'wolf_output'):
		"""
		Delocalizes the output directory of any relevant tasks.
		Returns a final pandas dataframe containing only the declared outputs.
		"""

		def listize(obj):
			if isinstance(obj, list):
				return obj
			return [obj]

		outputs_by_task = {
			taskname: [(name, getter) for name, getter in outputs.items() if getter.taskname == taskname]
			for taskname in {getter.taskname for getter in outputs.values()}
		}
		if not os.path.exists(output_dir):
			os.makedirs(output_dir)
		flow_outputs = pd.DataFrame(columns=['task', 'field_name', 'files'], index=pd.Index([], name='output'))
		for taskname in outputs_by_task:
			taskdir = os.path.join(output_dir, taskname)
			if len(outputs_by_task[taskname]):
				task_outputs = outputs_by_task[taskname][0][1].task.localizer.delocalize(
					{
						field: getter.task.conf['outputs'][field]
						for name, getter in outputs_by_task[taskname] for field in listize(getter.fields)
					},
					taskdir
				)
				print(task_outputs)
				flow_outputs = flow_outputs.append(
					pd.DataFrame(data=[
						{
							'task': taskname,
							'output': name,
							'field_name': field,
							'files': [
								path for job_outputs in task_outputs.values() for path in job_outputs[field]
							]
						}
						for name, getter in outputs_by_task[taskname] for field in listize(getter.fields)
					]).set_index('output')[flow_outputs.columns]
				)
		return flow_outputs

	def _index_tasks(self):
		print("My strategy is", self.strategy)
		task_index = {}
		for member in self.__dict__.values():
			if isinstance(member, Task):
				# add backend to task if not present
				if member.backend is None:
					member.backend = self.backend # Ah
					print("INDEXING", member.conf['name'])
					print(member.conf['localization'])
					member.conf['localization'] = {
						**{'strategy': self.strategy},
						**member.conf['localization']
					}
					print(member.conf['localization'])

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
			for name, workflow in self.flow_list.items():
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

				# AG: Not sure what to do with this right now
				# For now, let's rewrite self.outputs with actual outputs
				self.outputs[name] = self.outputs[name][0].delocalize(self.outputs[name][1]) if self.outputs[name][1] is not None else None

				w.append(pd.concat(r, keys = workflow.keys(), names = ["task", "shard"]))

			# XXX: we might need to put this in a try/catch block -- there are
			#      many more ways that this can get screwed up besides zero output.
			self.results = pd.concat(w, keys = self.flow_list.keys(), names = ["workflow_run_name"]) if len(w) > 0 else None

			# TODO: save results
		except KeyboardInterrupt:
			# TODO: can we register SIGTERM to raise a keyboard interrupt so this
			#       exception can behave identically?
			print("\nWorkflow cancelled by user!")
			print("Stopping all jobs, please wait ...")

			#
			# asynchronously cancel all tasks
			cancel_threads = []
			for workflow in self.flow_list.values():
				for task in workflow.values():
					th = threading.Thread(target = task.cancel)
					th.start()
					cancel_threads.append(th)
			for th in cancel_threads:
				th.join(timeout = 60)
		finally:
			print("Tearing down cluster, please wait ...")
			self.backend.__exit__()
