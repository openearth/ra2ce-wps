# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2022 Deltares
#
#
#       Ioanna Micha
#       ioanna.micha@deltares.nl
#
#
#   This library is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this library.  If not, see <http://www.gnu.org/licenses/>.
#   --------------------------------------------------------------------
#
# This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
# OpenEarthTools is an online collaboration to share and manage data and
# programming tools in an open source, version controlled environment.
# Sign up to recieve regular updates of this function, and to contribute
# your own tools.

# Reclassify the kans layer. (Probability)

# other
from pywps import Process, Format
from pywps.inout.outputs import ComplexOutput
from pywps.app.Common import Metadata  # to do
import json
import logging
from sqlalchemy import create_engine

# local
from .ra2ce_utils import read_config


class WpsRa2ceReclassifyProbability(Process):
    def __init__(self):
        # Input [in json format] - no inputs
        inputs = []  # no inputs

        # Output [in json format]
        outputs = [
            ComplexOutput(
                "output_json",
                "Ra2ce operator cost layer from selected hazard",
                supported_formats=[Format("application/json")],
            )
        ]

        super(WpsRa2ceReclassifyProbability, self).__init__(
            self._handler,
            identifier="ra2ce_reclassify_probability",
            version="1.0",
            title="backend process for the RA2CE 2.0 version, reclassifies the probability layer",
            abstract="It reclassifies the probability layer based on the given classes borders",
            profile="",
            inputs=inputs,
            outputs=outputs,
            store_supported=False,
            status_supported=False,
        )

    ## MAIN
    def _handler(self, request, response):

        # try:

        # 1. Read configuration
        cf = read_config()

        check = (
            "postgresql+psycopg2://"
            + cf.get("PostGis", "user")
            + ":"
            + cf.get("PostGis", "pass")
            + "@"
            + cf.get("PostGis", "host")
            + ":"
            + str(cf.get("PostGis", "port"))
            + "/"
            + cf.get("PostGis", "db")
        )

        # 2. connect to database
        engine = create_engine(
            "postgresql+psycopg2://"
            + cf.get("PostGis", "user")
            + ":"
            + cf.get("PostGis", "pass")
            + "@"
            + cf.get("PostGis", "host")
            + ":"
            + str(cf.get("PostGis", "port"))
            + "/"
            + cf.get("PostGis", "db"),
            strategy="threadlocal",
        )

        # 3. read from request hazard_id and value_ranges

        # 4. hazard_id has the same name as the table in the database, so ra2ce_2_0.hazard_id create a new table with a temp name
        # 5. Update table.
        # 6. Upload new geoserver layer with same name as the temp table on geoserver with styling name equal to the classes range, eg 5 6, 7 8
        # 7. based on that table create a new probabilities table. similar as in the past. so from here and after procedure same? In the past there was already a probability table for each case. So I need to prepare a default one.
        # I never show the default one. I always get it create a temp based on that one blah blah. same procedure in other words.

        # except Exception as e:
        #    res = {"errMsg": "ERROR: {}".format(e)}
        #    logging.info("""WPS [WpsRa2ceProvideHazardList]: ERROR = {}""".format(e))
        #    response.outputs["output_json"].data = json.dumps(res)

        return response
