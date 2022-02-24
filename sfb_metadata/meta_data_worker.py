import re
import os
import warnings

import numpy as np
import pandas as pd
from datetime import datetime
from sfb_metadata.coscine_overview import CoscineOverview
from sfb_metadata.utils import Compound


class WorkCoscineOverview:
    def __init__(self, coscine_overview: CoscineOverview):
        self._coscine_overview = coscine_overview
        self._schemes = {}
        self.debug = False

        self._sort_res_into_schemes()

    def _sort_res_into_schemes(self):
        for idx, res in enumerate(self.resources):
            scheme_name = self._get_profile(res)
            if scheme_name not in self._schemes:
                self._schemes[scheme_name] = []
            self._schemes[scheme_name].append(idx)
        if len(self._schemes) == 0:
            raise ValueError("No data available in the coscine_overview!")

    def get_file_handle(self, file=None, pr_id=None, res_id=None, file_name=None):
        if self.client is None:
            raise RuntimeError("No coscine_client available! specify client=coscine.Client or client=TOKEN")
        if file is None:
            if pr_id is None or res_id is None or file_name is None:
                raise ValueError()
        else:
            if pr_id is not None or res_id is not None or file_name is not None:
                raise ValueError()
            if isinstance(file, int):
                pr_id = self.projects[self.files[file]['project']]['id']
                res_id = self.resources[self.files[file]['resource']]['id']
                file_name = self.files[file]['id']
            elif isinstance(file, dict):
                pr_id = self.projects[file['project']]['id']
                res_id = self.resources[file['resource']]['id']
                file_name = file['id']
            elif isinstance(file, pd.DataFrame):
                pr_id = file['pr_id'][0]
                res_id = file['res_id'][0]
                file_name = file['file name'][0]
                if len(file) > 1:
                    warnings.warn("More than one record! Providing only the first file handle!")
            else:
                raise TypeError(f"Unknown type {type(file)}.")

        if self.debug:
            print(f"DEBUG get file handle:\n   pr_id={pr_id}\n   res_id={res_id}\n   file_name={file_name}")
        pr = self.client.projects(toplevel=False, id=pr_id)[0]
        res = pr.resources(id=res_id)[0]
        obj = res.objects(Name=file_name)
        return obj[0]

    def get_file_content(self, file):
        return self.get_file_handle(file).content()

    @staticmethod
    def _get_profile(resource):
        return resource['profile'].split('/')[-2]

    @property
    def client(self):
        return self._coscine_overview.client

    @client.setter
    def client(self, new_client):
        self._coscine_overview.client = new_client

    @property
    def projects(self):
        return self._coscine_overview.projects

    @property
    def resources(self):
        return self._coscine_overview.resources

    @property
    def files(self):
        return self._coscine_overview.files

    @property
    def scheme_list(self):
        return list(self._schemes.keys())

    def get_resources_for_scheme(self, scheme, list_empty_resources=True):
        if scheme not in self._schemes:
            raise ValueError(f"No scheme '{scheme}' available. Choose one of {self.scheme_list}.")
        result = []
        for res_idx in self._schemes[scheme]:
            res = self.resources[res_idx]
            if list_empty_resources or len(res["files"]) > 0:
                result.append(res)
        return result

    def _get_file_idx_for_scheme(self, scheme):
        result = []
        for res in self.get_resources_for_scheme(scheme=scheme, list_empty_resources=False):
            result += res["files"]
        return result

    def get_files_for_scheme(self, scheme):
        result = []
        for file_idx in self._get_file_idx_for_scheme(scheme):
            result.append(self.files[file_idx])
        return result

    def get_file_idx(self, source):
        """Receive list of file indices for source

        Args:
            source(str/list/resource/int): Source of the files to consider. May be given as:
                                            the name of the scheme (string)
                                            list of resources
                                            list of integers corresponding to indices in self.resources
                                            one resource
                                            one integer corresponding to an index in self.resources
        """
        if isinstance(source, str):
            files = self._get_file_idx_for_scheme(source)
        elif isinstance(source, list) and isinstance(source[0], dict):
            files = []
            for res in source:
                files += res["files"]
        elif isinstance(source, list) and isinstance(source[0], int):
            files = []
            for res_idx in source:
                files += self.resources[res_idx]["files"]
        elif isinstance(source, dict):
            files = source["files"]
        elif isinstance(source, int):
            files = self.resources[source]["files"]
        else:
            raise TypeError(source)

        return files

    def get_metadata(self, source, parse_sample_comments=True):
        result = []
        file_idx_list = self.get_file_idx(source)
        _res_list = [self.files[file_idx_list[0]]['resource']]
        profile_name = self._get_profile(self.resources[_res_list[0]])
        for file_id in file_idx_list:
            file = self.files[file_id]
            res_id = file['resource']
            if res_id not in _res_list:
                if self._get_profile(self.resources[res_id]) != profile_name:
                    raise ValueError("Resources belong to more than one scheme!")
                else:
                    _res_list.append(res_id)

            data = {
                'file name': file['name'],
                'file type': os.path.splitext(file['name'])[1],
                'file size': file['size'],
                'file path': file['path'],
                'res_id': self.resources[file["resource"]]['id'],
                'pr_id': self.projects[file['project']]['id'],
            }
            data.update(file['metadata'])
            result.append(data)
        if profile_name == 'Sample' and parse_sample_comments:
            return self.extend_sample_comments(pd.DataFrame(result))
        else:
            return pd.DataFrame(result)

    @staticmethod
    def _sample_comment_parser(sample_comment: str):
        comment_lines = sample_comment.split('\n')[2:]
        data = {}
        temp_dicts = {}
        special_keys = {
            "Annealing": 'anneal',
            "Actual wt.%": "actual_wt",
            "Target wt.%": 'target wt.%',
            'Target at.%': 'target at.%',
        }

        for line_no, line in enumerate(comment_lines):
            if line.startswith(tuple(special_keys.keys())):
                for _key in special_keys:
                    if line.startswith(_key):
                        key = _key
                spl = line[len(key):].split(':')
                values = [s.strip() for s in spl[1:]]
                for i, value in enumerate(values):
                    try:
                        values[i] = float(value)
                    except:
                        pass
                if len(values) == 1:
                    values = values[0]
                temp_dicts[(key, spl[0].strip())] = values
            elif line.startswith('Date'):
                spl = line.split(':')
                try:
                    data[('Creation Date', '')] = datetime.fromisoformat(':'.join(spl[1:]).strip())
                except ValueError:
                    if spl[1].strip() == 'NaT':
                        data[('Creation Date', '')] = None
                    else:
                        data[('Creation Date', '')] = ':'.join(spl[1:]).strip()
            else:
                spl = line.split(':')
                if spl[0] != '':
                    data[(spl[0], '')] = ':'.join(spl[1:]).strip()

        for tmp_dict_name in temp_dicts:
            data[tmp_dict_name] = temp_dicts[tmp_dict_name]
        return data

    def _sample_parser(self, sample_df):
        comments = []
        for idx, row in sample_df.iterrows():
            try:
                comments.append(self._sample_comment_parser(row.Comments))
            except AttributeError as e:
                comments.append({('Comments', ""): row.Comments})
        return comments

    def extend_sample_comments(self, sample_df: pd.DataFrame):
        _sample_df = sample_df.copy()
        parsed_df = pd.DataFrame(self._sample_parser(_sample_df))
        parsed_df = pd.DataFrame(parsed_df.values, columns=pd.MultiIndex.from_tuples(parsed_df.keys()))
        _sample_df.drop(columns='Comments', inplace=True)
        _sample_df = pd.DataFrame(_sample_df.values,
                                  columns=pd.MultiIndex.from_tuples([(key, "") for key in _sample_df.keys()]))
        return pd.concat([_sample_df, parsed_df], axis=1)

    @staticmethod
    def _parse_div_string(div_string: str):
        result = {}
        spl = re.findall("[0-9,]+ *[A-Za-z]+", div_string)
        for s in spl:
            result[re.search("[A-Za-z]+", s).group()] = re.search("[0-9,]+ *", s).group()
        return result

    def get_T_c(self, sample_df: pd.DataFrame, only_actual_c=False, debug=False, expand_c_base=True):
        result = sample_df[['ID']]  # , 'file name', 'res_id', 'pr_id']]
        c_dict = {}
        if only_actual_c:
            c_lookup_keys = ['Actual wt.%']
        else:
            c_lookup_keys = ['Target wt.%', 'Actual wt.%']
        for sub_df in c_lookup_keys:
            for idx, row in sample_df[sub_df].iterrows():
                if idx not in c_dict:
                    c_dict[idx] = {}
                elif debug:
                    print(f'DEBUG: prior: {c_dict[idx]}')
                tmp_result = {}
                for key, value in row.items():
                    if (isinstance(value, float) and pd.notna(value)) or value == "base":
                        tmp_result[('wt.%', key)] = value
                        if debug:
                            print(f"entered first condition with value={value}, type={type(value)}, check " +
                                  f"is np.nan {value is np.nan} == np.nan {value == np.nan} , {pd.notna(value)}")
                    if key == 'Div.' and value is not np.nan and value != 'nm':
                        if debug:
                            print(f"entered 2nd condition with value={value}")
                        for _key, _val in self._parse_div_string(value).items():
                            tmp_result[('wt.%', _key)] = float(_val.strip().replace(',', '.'))
                if sub_df == 'Target at.%' and len(tmp_result) > 0:
                    compound = Compound({key[1]: value for key, value in tmp_result.items()})
                    tmp_result = {('wt.%', key): value for key, value in compound.wt_percent_dict.items()}
                c_dict[idx].update(tmp_result)
                if debug:
                    print(f"sub_df= {sub_df} :{c_dict[idx]}")

        t_dict = {}
        for sub_df in ['Reduction temp[°C]', ("Annealing", 'Temp.[°C]')]:
            for idx, value in sample_df[sub_df].items():
                if (isinstance(value, (float, int)) and value is not np.nan):
                    t_dict[idx] = {('T', ""): value}
                elif isinstance(value, str) and value != '-':
                    try:
                        t_dict[idx] = {('T', ""): float(value)}
                    except:
                        t_dict[idx] = {('T info', ""): value}

        result = pd.concat([result, pd.DataFrame(c_dict).T, pd.DataFrame(t_dict).T], axis=1)

        if expand_c_base:
            result.apply(self._expand_c_base, axis=1)

        return result

    @staticmethod
    def _expand_c_base(pd_series):
        row = pd_series['wt.%']
        base_sum = 100.0 - sum(row[row != 'base'].dropna())
        pd_series[pd_series == 'base'] = base_sum
        return pd_series
