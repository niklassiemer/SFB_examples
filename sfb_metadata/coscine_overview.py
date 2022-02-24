import warnings

import coscine
import os
from pyiron_base import FileHDFio
import json


class CoscineOverview:
    def __init__(self, token=None, verbose_level=0):
        self._client = None
        self.verbose_level = verbose_level
        self._init_data_fields()
        self.fail_hard = False
        self._hdf = FileHDFio(file_name=os.path.join(os.getcwd(), "CoScInE_Overview"))
        if token is not None:
            self._init_coscine_client(token)
        if self._hdf.file_exists:
            try:
                self.from_hdf()
            except Exception as e:
                warnings.warn(
                    f"HDF file found but could not be loaded due to {e.__class__.__name__}{e.args})! "
                    f"No data available! Rerun download_from_coscine!"
                )
        else:
            warnings.warn("No data loaded, run download_from_coscine first.")

    def _init_data_fields(self):
        self._projects = []
        self._files = []
        self._resources = []
        self._file_handles = {}
        self._log = []
        self._errors = []

    @property
    def projects(self):
        return self._projects

    @property
    def files(self):
        return self._files

    @property
    def resources(self):
        return self._resources

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, new_client):
        self._init_coscine_client(new_client)

    def download_from_coscine(self, token=None, verbose_level=None):
        self._init_data_fields()
        if token is not None:
            self._init_coscine_client(token)
        if verbose_level is not None:
            self.verbose_level = verbose_level
        for pr in self._client.projects():
            self._gen_pr_entry(pr, "")

        self.to_hdf()

    def to_hdf(self, hdf=None):
        if hdf is not None:
            self._hdf = hdf
        self._hdf["projects"] = json.dumps(self._projects)
        self._hdf["files"] = json.dumps(self._files)
        self._hdf["resources"] = json.dumps(self._resources)

    def from_hdf(self, hdf=None):
        if hdf is not None:
            self._hdf = hdf
        self._projects = json.loads(self._hdf["projects"])
        self._files = json.loads(self._hdf["files"])
        self._resources = json.loads(self._hdf["resources"])

    def _init_coscine_client(self, token):
        if isinstance(token, str):
            self._client = coscine.Client(token, verbose=False)
        elif isinstance(token, coscine.Client):
            self._client = token
        else:
            raise TypeError(f"Expected str or coscine.Client but got {type(token)}")
        self._client.projects()

    def _coscine_query(self, coscine_object, method_name, *args, **kwargs):
        method = getattr(coscine_object, method_name)
        if not callable(method):
            return method
        else:
            try:
                return method(*args, **kwargs)
            except coscine.CoscineException as e:
                self._errors.append(e)
                msg = f"Error for `{coscine_object.__class__}.{method_name}({args}, {kwargs})` with {e.__class__.__name__}('{e}')"
                self._log.append(msg)
                if self.fail_hard:
                    raise e
        return []

    def _gen_pr_entry(self, project: coscine.Project, path, parent_project_id=None):
        project_dict = {
            # 'project': project,
            "id": project.id,
            "path": path,
            "name": project.name,
            "parent": parent_project_id,
        }
        self_idx = len(self._projects)
        self._projects.append(project_dict)
        path += "/" + project.name

        if self.verbose_level:
            print(f"Project: {project.name} at {path}")

        res_list = []
        for res in self._coscine_query(project, "resources"):
            res_list.append(self._gen_res_entry(res, path, self_idx))
        project_dict["resources"] = res_list

        sub_projects = []
        for pr in self._coscine_query(project, "subprojects"):
            sub_projects.append(self._gen_pr_entry(pr, path, self_idx))
        project_dict["sub_projects"] = sub_projects

        return self_idx

    def _gen_res_entry(self, res: coscine.Resource, path, pr_idx):
        self_idx = len(self._resources)
        result = {}
        self._resources.append(result)
        res_path = path + "/" + res.name
        if self.verbose_level > 1:
            print(f"  Resource {res.name} at {res_path}")
        result["id"] = res.id
        result["path"] = res_path
        result["project"] = pr_idx
        # result["resource"] = res
        result["name"] = res.name
        result["profile"] = res.data["applicationProfile"]
        file_list = []
        for file in self._coscine_query(res, "objects"):
            file_list.append(self._gen_file_entry(file, res_path, self_idx, pr_idx))
        result["files"] = file_list
        result["size"] = sum([self._files[file_id]["size"] for file_id in file_list])
        return self_idx

    def _gen_file_entry(self, file: coscine.Object, path, res_idx, pr_idx):
        if self.verbose_level > 2:
            print(f"    File {file.name} in resource {path}")
        self_idx = len(self._files)
        result = {}
        self._files.append(result)

        file_path = path + "/" + file.name
        result["id"] = file.name
        result["path"] = file_path
        self._file_handles[self_idx] = file
        result["name"] = file.name
        try:
            result["metadata"] = file.form().store
        except Exception as e:
            self._errors.append(e)
            msg = f"Problem for receiving metadata for file {file.name} with {e.__class__.__name__}('{e}')"
            self._log.append(msg)
            if self.verbose_level > 0:
                print("    ", msg)
            if self.fail_hard:
                raise e
            result["metadata"] = {
                "error": e,
                "err_call": f"{e.__class__.__name__}('{e}')",
                "error_context_args": e.__context__.args,
            }
        result["size"] = file.size
        result["project"] = pr_idx
        result["resource"] = res_idx
        return self_idx
