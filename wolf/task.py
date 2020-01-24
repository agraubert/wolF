import canine

import copy
import os
import pandas as pd
import random
import re
import threading
import traceback
import typing

class Task:
	pass

class Task:
	@staticmethod
	def process_deps(deps):
		depstr = []
		for d in deps:
			if isinstance(d, Task):
				dep = str(d.batch_id)
				if dep == "-1":
					raise ValueError("Upstream dependency {} was unable to even start.".format(d.conf["name"]))
				elif dep == "-2":
					print("Upstream dependency \"{}\" was completely job avoided.".format(d.conf["name"]))
				else:
					depstr.append(dep)
			elif d is None:
				pass
			else:
				raise TypeError("Dependencies can only be specified as task objects!")

		# did we have non-avoided dependencies?
		if len(depstr) > 0:
			# TODO: support additional dependency types (e.g. afternotok)
			return "after:" + ":".join(depstr)
		else:
			return None

	def __init__(
	  self, *,
	  name: str,
	  inputs: typing.Optional[typing.Dict] = None,
	  outputs: typing.Optional[typing.Dict] = None,
	  resources: typing.Dict = { "cpus-per-task" : 1, "mem" : "1G" },
	  script: typing.Union[str, typing.List[str], typing.Callable, typing.List[typing.Callable]],
	  overrides: typing.Optional[typing.Dict] = None,
	  output_dir: typing.Optional[str] = None,
	  output_dir_stem: str = "/mnt/nfs/workspace/",
	  conf: typing.Optional[typing.Dict] = None,
	  backend: typing.Optional[canine.backends.AbstractSlurmBackend] = None,
	  dependencies:
	    typing.Optional[typing.Union[str, typing.List[str], Task, typing.List[Task]]] = None,
	  extra_slurm_args: typing.Optional[typing.Dict] = {},
	  docker: typing.Optional = None
	):
		try:
			#
			# define config dict
			if conf is not None:
				self.conf = copy.deepcopy(conf)
			else:
				self.conf = {
				  "localization" : { "strategy" : "NFS" },
				  "script" : []
				}

			self.conf["name"] = name

			self.conf["inputs"] = inputs
			self.conf["outputs"] = outputs

			self.conf["resources"] = resources

			if overrides is not None:
				self.conf["localization"]["overrides"] = overrides

			#
			# script
			if "script" not in self.conf:
				self.conf["script"] = []

			if not callable(script):
				if type(script) == str:
					script = [script]

				self.conf["script"].extend(script)
			else:
				self.conf["script"] = script

			self.backend = backend if backend is not None else None

			#
			# staging dir
			self.output_dir_stem = output_dir_stem
			self.output_dir = re.sub("/", "-", self.conf["name"]) if output_dir is None else output_dir
			self.set_output_directory(self.output_dir)

			#
			# dependencies
			if dependencies is not None:
				self.dependencies = dependencies if type(dependencies) == list else [dependencies]
			else:
				self.dependencies = []

			#
			# extra Slurm arguments
			self.extra_slurm_args = extra_slurm_args

			#
			# Docker parameters
			self.docker = docker
			if self.docker is not None:
				if type(self.docker) == dict:
					if "image" not in self.docker:
						raise ValueError("Must specify Docker image!")
				elif type(self.docker) == str:
					self.docker = { "image" : self.docker }
				else:
					raise TypeError("Docker must be specified as a dict or string!")

			#
			# create placeholders: 

			# batch ID of submitted job
			self.batch_id = None

			# output dataframe of last run submission
			self.results = pd.DataFrame(columns = self.conf["outputs"])

			# number of jobs avoided
			self.n_avoided = 0

			# runner thread
			self.thread = None

			#
			# create and acquire lock; will be released when task finishes
			self.lock = threading.Lock()
			self.lock.acquire()
		except:
			print("Error initializing job {}:".format(name))
			traceback.print_exc()

			self.batch_id = -1

	def set_output_directory(self, outdir):
		output_dir = os.path.join(self.output_dir_stem, outdir)
		self.conf["localization"]["staging_dir"] = output_dir

	def get_output(self, fields, func = lambda x : x):
		def output_getter():
			return func(self.results["outputs"].loc[:, fields])

		return output_getter

	def block_and_convert_deps(self):
		#
		# block on dependencies
		for dep in self.dependencies:
			if dep is not None:
				dep.lock.acquire()
				dep.lock.release()

		print("Task {} dependencies satisfied".format(self.conf["name"]))

		#
		# we need to call input closures, and convert the series that gets returned
		# to a list
		for k, v in self.conf["inputs"].items():
			if callable(v):
				if v.__name__ == "output_getter":
					self.conf["inputs"][k] = v().tolist()
				else:
					self.conf["inputs"][k] = v()

	def run(self):
		if self.thread is not None and self.thread.is_alive():
			print("Task is currently running; if you wish to restart, please cancel and re-run.")
			return

		if callable(self.conf["script"]):
			def func_closure():
				self.block_and_convert_deps()
				self.results = self.conf["script"](self.conf["inputs"])
				self.lock.release()

			targ = func_closure
		else:
			targ = self._run

		self.thread = threading.Thread(target = targ)
		self.thread.start()

	def _run(self):
		def exception(gerund):
			print("Error {} job {}:".format(gerund, self.conf["name"]))
			traceback.print_exc()

			self.batch_id = -1
			if self.lock.locked():
				self.lock.release()

		try:
			#
			# check if backend is valid
			# TODO: should we check if it's been entered? 
			if self.backend is None:
				raise Exception("Backend has not yet been initialized!")

			#
			# block on dependencies
			self.block_and_convert_deps()

			#
			# process dependencies
			if len(self.dependencies) > 0:
				try:
					proc_deps = Task.process_deps(
						self.dependencies if type(self.dependencies) == list else [self.dependencies]
					)
				except:
					print("Upstream dependencies in {} were unable to start!".format(self.conf["name"]))
					raise

				if proc_deps is not None:
					self.extra_slurm_args = {
					  **self.extra_slurm_args,
					  **{"dependency" : proc_deps }
					}
		except:
			exception("initializing")
			raise

		if self.docker is not None:
			try:
				#
				# do Docker stuff (if necessary)

				# set Docker defaults
				for k, v in {
				  "tag" : "latest",
				  "shell" : "/bin/bash",
				  "rm" : True
				}.items():
					if k not in self.docker:
						self.docker[k] = v

				# wrap script in Docker invocation
				delim = '%08x' % random.randrange(0xFFFFFFFF)
				self.conf["script"] = [
				  'docker run -v /mnt/nfs:/mnt/nfs {rm} --network host -i \
				  --name "{name}" --user $(id -u {user}):$(id -g {user}) \
				  -e "K9_CWD=`pwd`" --env-file <(env | cut -f 1 -d =) {image} {shell} - <<'.format(
					name = self.conf["name"] + "_$SLURM_ARRAY_TASK_ID",
					user = self.backend.config["user"] if "user" not in self.docker \
					  else self.docker["user"],
					image = self.docker["image"] + ":" + self.docker["tag"],
				    shell = self.docker["shell"],
				    rm = "--rm" if self.docker["rm"] else ""
				  ) + \
				  '"' + delim + '"\n' + "cd $K9_CWD\n" + "\n".join(self.conf["script"]) + \
				  "\n" + delim
				]
			except:
				exception("configuring Docker")
				raise

		try:
			#
			# initialize orchestrator and localizer
			self.orch = canine.Orchestrator(self.conf)
			self.orch.backend = self.backend
			self.localizer = canine.localization.nfs.NFSLocalizer(self.orch.backend, **self.conf["localization"])

			# localize files
			self.n_avoided = self.orch.job_avoid(self.localizer)
			entrypoint_path = self.orch.localize_inputs_and_script(self.localizer)
		except:
			exception("localizing files for")
			raise

		completed_jobs = cpu_time = uptime = prev_acct = None
		try:
			# submit batch job
			self.batch_id = self.orch.submit_batch_job(entrypoint_path, self.localizer.environment('compute'), self.extra_slurm_args)

			# wait for jobs to finish
			completed_jobs, cpu_time, uptime, prev_acct = self.orch.wait_for_jobs_to_finish(self.batch_id)

			# FIXME: currently, job accounting does not work with partially avoided
			#        jobs. this is because Task.status() assumes Task.orch.job_spec
			#        is in sync with the array IDs

			if self.n_avoided == 0:
				# job comprised a single task
				if len(self.orch.job_spec) == 1:
					print("Task \"{}\" finished with status {}".format(
					  self.conf["name"], self.status()[("job", "State")].iloc[0]
					))
				# job comprised multiple tasks
				elif len(self.orch.job_spec) >= 1:
					print("Task \"{}\" finished with statuses {}".format(
					  self.conf["name"],
					  ", ".join([str(k) + ": " + str(v)
						for k, v in self.status()[("job", "State")].value_counts().iteritems()
					  ])
					))
			else:
				# TODO: also print the total number of jobs in this task
				print("Task \"{}\" was job avoided ({:d} jobs avoided).".format(
				  self.conf["name"],
				  self.n_avoided
				))
				
		except:
			exception("running")
			raise
		finally:
			# delocalize files
			outputs = self.localizer.delocalize(self.orch.raw_outputs)

			# make output dataframe
			self.results = self.orch.make_output_DF(self.batch_id, outputs, cpu_time, prev_acct, self.localizer)

			# release lock
			if self.lock.locked():
				self.lock.release()

	def cancel(self):
		while self.batch_id is None:
			pass

		self.backend.scancel(self.batch_id)

	def status(self):
		# FIXME: we need to be able to report status on partially avoided jobs
		#        of course, it still makes sense to not report anything on
		#        completely avoided jobs.
		if self.batch_id != -2 and self.n_avoided == 0:
			sacct_df = self.backend.sacct(job = self.batch_id, format = "JobId,JobName,State,ExitCode,CPUTimeRaw,NodeList%30")
			sacct_df = sacct_df.loc[sacct_df["JobName"] != "batch"].drop(columns = "JobName")
			sacct_df = pd.concat([
			  sacct_df,
			  sacct_df.index.str.extract(r"(\d+)_(\d+)").rename(
				columns = dict(enumerate(["JID", "ArrayID"]))
			  ).set_index(sacct_df.index)
			], 1)
			sacct_df.columns = pd.MultiIndex.from_product([["job"], sacct_df.columns])

			# merge with inputs
			input_df = pd.DataFrame.from_dict(self.orch.job_spec, orient = "index")
			input_df.columns = pd.MultiIndex.from_product([["inputs"], input_df.columns])

			sacct_df = sacct_df.merge(input_df, left_on = [("job", "ArrayID")], right_index = True)

			return sacct_df
		else:
			print("Job was avoided; no status to report.")
