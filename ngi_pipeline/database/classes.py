from __future__ import print_function

import functools
import json
import os
import re
import requests

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import memoized

# Need a better way to log
LOG = minimal_logger(__name__)


try:
    CHARON_API_TOKEN = os.environ['CHARON_API_TOKEN']
    CHARON_BASE_URL = os.environ['CHARON_BASE_URL']
    # Remove trailing slashes
    m = re.match(r'(?P<url>.*\w+)/*', CHARON_BASE_URL)
    if m:
        CHARON_BASE_URL = m.groups()[0]
except KeyError as e:
    raise ValueError("Could not get required environmental variable "
                     "\"{}\"; cannot connect to database.".format(e))


## TODO Might be better just to instantiate this when loading the module. Do we neeed a new instance every time? I don't think so
class CharonSession(requests.Session):
    def __init__(self, api_token=None, base_url=None):
        super(CharonSession, self).__init__()

        self._api_token = api_token or CHARON_API_TOKEN
        self._api_token_dict = {'X-Charon-API-token': self._api_token}
        self._base_url = base_url or CHARON_BASE_URL

    #def get(url_args, *args, **kwargs):
    #    url = self.construct_charon_url(url_args)
    #    return validate_response(super(CharonSession, self).get(url,
    #                                   headers=self._api_token_dict,
    #                                   *args, **kwargs))

        self.get = validate_response(functools.partial(self.get, headers=self._api_token_dict))
        self.post = validate_response(functools.partial(self.post, headers=self._api_token_dict))
        self.put = validate_response(functools.partial(self.put, headers=self._api_token_dict))
        self.delete = validate_response(functools.partial(self.delete, headers=self._api_token_dict))

        self._project_params = ("projectid", "name", "status", "pipeline", "bpa")
        self._sample_params = ("sampleid", "status", "received", "qc_status",
                               "genotyping_status", "genotyping_concordance",
                               "lims_initial_qc", "total_autosomal_coverage")
        self._libprep_params = ("libprepid", "limsid", "status")
        self._seqrun_params = ('seqrunid', 'sequencing_status', 'alignment_status',
                               'runid', 'seq_qc_flag', 'demux_qc_flag',
                               'mean_coverage', 'std_coverage', 'GC_percentage',
                               'aligned_bases', 'mapped_bases', 'mapped_reads',
                               'total_reads', 'sequenced_bases', 'windows', 'bam_file',
                               'output_file', 'mean_mapping_quality', 'bases_number',
                               'contigs_number', 'mean_autosomal_coverage', 'lanes',
                               'alignment_coverage', 'reads_per_lane')
        self._seqrun_reset_params = tuple(set(self._seqrun_params) - \
                                          set(['demux_qc_flag', 'lanes', 'windows', 'seq_qc_flag',
                                               'alignment_coverage', 'alignment_status',
                                               'sequencing_status', 'total_reads', 'runid', 'seqrunid']))


    ## Another option is to build this into the get/post/put/delete requests
    ## --> Do we ever need to call this (or those) separately?
    @memoized
    def construct_charon_url(self, *args):
        """Build a Charon URL, appending any *args passed."""
        return "{}/api/v1/{}".format(self._base_url,'/'.join([str(a) for a in args]))


    ## FIXME There's a lot of repeat code here that might could be condensed

    # Project
    def project_create(self, projectid, name=None, status=None, pipeline=None, bpa=None):
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._project_params }
        return self.post(self.construct_charon_url('project'),
                         data=json.dumps(data)).json()

    def project_get(self, projectid):
        return self.get(self.construct_charon_url('project', projectid)).json()


    def project_get_samples(self, projectid):
        return self.get(self.construct_charon_url('samples', projectid)).json()
    
    def project_update(self, projectid, name=None, status=None, pipeline=None, bpa=None):
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._project_params if l_dict.get(k)}
        return self.put(self.construct_charon_url('project', projectid),
                        data=json.dumps(data)).text

    def projects_get_all(self):
        return self.get(self.construct_charon_url('projects')).json()

    def project_delete(self, projectid):
        return self.delete(self.construct_charon_url('project', projectid)).text

    # Sample
    def sample_create(self, projectid, sampleid, status=None, received=None,
                      qc_status=None, genotyping_status=None,
                      genotyping_concordance=None, lims_initial_qc=None,
                      total_autosomal_coverage=None):
        url = self.construct_charon_url("sample", projectid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._sample_params }
        return self.post(url, json.dumps(data)).json()

    def sample_get(self, projectid, sampleid):
        url = self.construct_charon_url("sample", projectid, sampleid)
        return self.get(url).json()

    def sample_get_libpreps(self, projectid, sampleid):
        return self.get(self.construct_charon_url('libpreps', projectid, sampleid)).json()

    def sample_update(self, projectid, sampleid, status=None, received=None,
                      qc_status=None, genotyping_status=None,
                      genotyping_concordance=None, lims_initial_qc=None,
                      total_autosomal_coverage=None):
        url = self.construct_charon_url("sample", projectid, sampleid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._sample_params if l_dict.get(k)}
        return self.put(url, json.dumps(data)).text

    ## Eliminate?
    def samples_get_all(self, projectid):
        return self.project_get_samples(projectid)
        #return self.get(self.construct_charon_url('samples', projectid)).json()

    # LibPrep
    def libprep_create(self, projectid, sampleid, libprepid, status=None, limsid=None):
        url = self.construct_charon_url("libprep", projectid, sampleid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._libprep_params }
        return self.post(url, json.dumps(data)).json()

    def libprep_get(self, projectid, sampleid, libprepid):
        url = self.construct_charon_url("libprep", projectid, sampleid, libprepid)
        return self.get(url).json()

    def libprep_get_seqruns(self, projectid, sampleid, libprepid):
        return self.get(self.construct_charon_url('seqruns', projectid, sampleid, libprepid)).json()


    def libprep_update(self, projectid, sampleid, libprepid, status=None, limsid=None):
        url = self.construct_charon_url("libprep", projectid, sampleid, libprepid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._libprep_params if l_dict.get(k)}
        return self.put(url, json.dumps(data)).text

    ## Eliminate?
    def libpreps_get_all(self, projectid, sampleid):
        return self.sample_get_libpreps(projectid, sampleid)
        #return self.get(self.construct_charon_url('libpreps', projectid, sampleid)).json()

    # SeqRun
    def seqrun_create(self, projectid, sampleid, libprepid, seqrunid,
                      total_reads, mean_autosomal_coverage, reads_per_lane=None,
                      sequencing_status=None, alignment_status=None, runid=None,
                      seq_qc_flag=None, demux_qc_flag=None, mean_coverage=None,
                      std_coverage=None, GC_percentage=None, aligned_bases=None,
                      mapped_bases=None, mapped_reads=None,
                      sequenced_bases=None, windows=None, bam_file=None,
                      output_file=None, mean_mapping_quality=None,
                      bases_number=None, contigs_number=None,
                      lanes=None,
                      alignment_coverage=None):
        url = self.construct_charon_url("seqrun", projectid, sampleid, libprepid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._seqrun_params }
        return self.post(url, json.dumps(data)).json()

    def seqrun_get(self, projectid, sampleid, libprepid, seqrunid):
        url = self.construct_charon_url("seqrun", projectid, sampleid, libprepid, seqrunid)
        return self.get(url).json()

    def seqrun_update(self, projectid, sampleid, libprepid, seqrunid,
                      total_reads=None, mean_autosomal_coverage=None, reads_per_lane=None,
                      sequencing_status=None, alignment_status=None, runid=None,
                      seq_qc_flag=None, demux_qc_flag=None, mean_coverage=None,
                      std_coverage=None, GC_percentage=None, aligned_bases=None,
                      mapped_bases=None, mapped_reads=None,
                      sequenced_bases=None, windows=None, bam_file=None,
                      output_file=None, mean_mapping_quality=None,
                      bases_number=None, contigs_number=None,
                      lanes=None, alignment_coverage=None,
                      *args, **kwargs):
        ## TODO Consider implementing for allathese functions
        if args: LOG.debug("Ignoring extra args: {}".format(", ".join(*args)))
        if kwargs: LOG.debug("Ignoring extra kwargs: {}".format(", ".join(["{}: {}".format(k,v) for k,v in kwargs.iteritems()])))
        url = self.construct_charon_url("seqrun", projectid, sampleid, libprepid, seqrunid)
        l_dict = locals()
        data = { k: str(l_dict.get(k)) for k in self._seqrun_params if l_dict.get(k)}
        return self.put(url, json.dumps(data)).text

    def seqrun_reset(self, projectid, sampleid, libprepid, seqrunid):
        url = self.construct_charon_url("seqrun", projectid, sampleid, libprepid, seqrunid)
        data = { k: None for k in self._seqrun_reset_params}
        return self.put(url, json.dumps(data)).text


    def seqruns_get_all(self, projectid, sampleid, libprepid):
        return self.libprep_get_seqruns(projectid, sampleid, libprepid)
        #return self.get(self.construct_charon_url('seqruns', projectid, sampleid, libprepid)).json()


## TODO create different CharonError subclasses for different codes (e.g. 400, 404)
class CharonError(RuntimeError):
    def __init__(self, message, status_code=None, *args, **kwargs):
        self.status_code = status_code
        super(CharonError, self).__init__(message, *args, **kwargs)


class validate_response(object):
    """
    Validate or raise an appropriate exception for a Charon API query.
    """
    def __init__(self, f):
        self.f = f
        ## Should these be class attributes? I don't really know
        self.SUCCESS_CODES = (200, 201, 204)
        # There are certainly more failure codes I need to add here
        self.FAILURE_CODES = {
                400: (CharonError, ("Charon access failure: invalid input "
                                    "data (reason '{response.reason}' / "
                                    "code {response.status_code} / "
                                    "url '{response.url}')")),
                404: (CharonError, ("Charon access failure: not found "
                                    "in database (reason '{response.reason}' / "
                                    "code {response.status_code} / "
                                    "url '{response.url}')")), # when else can we get this? malformed URL?
                405: (CharonError, ("Charon access failure: method not "
                                    "allowed (reason '{response.reason}' / "
                                    "code {response.status_code} / "
                                    "url '{response.url}')")),
                409: (CharonError, ("Charon access failure: document "
                                    "revision conflict (reason '{response.reason}' / "
                                    "code {response.status_code} / "
                                    "url '{response.url}')")),}

    def __call__(self, *args, **kwargs):
        response = self.f(*args, **kwargs)
        if response.status_code not in self.SUCCESS_CODES:
            try:
                err_type, err_msg = self.FAILURE_CODES[response.status_code]
            except KeyError:
                # Error code undefined, used generic text
                err_type = CharonError
                err_msg = ("Charon access failure: {response.reason} "
                           "(code {response.status_code} / url '{response.url}')")
            raise err_type(err_msg.format(**locals()), response.status_code)
        return response
