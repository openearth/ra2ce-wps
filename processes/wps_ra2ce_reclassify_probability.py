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
from pywps.inout.inputs import LiteralInput, ComplexInput
from pywps.inout.outputs import ComplexOutput
from pywps.app.Common import Metadata
from geoserver.catalog import Catalog
from flask import jsonify


import time
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
        inputs = [
            LiteralInput("hazard_id", "name of hazard in db", data_type="string"),
            ComplexInput(
                "value_ranges",
                "array of ranges of classes",
                supported_formats=[Format("application/json")],
            ),
            LiteralInput(
                "priorities_matrix",
                "matrix with priorities",
                data_type="string",
            ),
        ]
        outputs = [
            ComplexOutput(
                "output_json",
                "--",
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
        # TODO: re-arrange in several re-usable functions
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
        hazard_id = request.inputs["hazard_id"][0].data
        ranges = json.loads(request.inputs["value_ranges"][0].data)

        priorities_matrix_str = request.inputs["priorities_matrix"][0].data

        priorities_matrix = json.loads(priorities_matrix_str)

        # TODO: create a function with below
        # 4. hazard_id has the same name as the table in the database, so ra2ce_2_0.hazard_id create a new table with a temp name
        temp_id = int(1000000 * time.time())
        temp_layer = f"{hazard_id}{temp_id}"
        print(temp_layer)

        strSql = f"create table temp.{temp_layer} as select id, geom, fid, kans, kans_klasse, amsheep_klasse from ra2ce_2_0.{hazard_id};"
        resSql = engine.execute(strSql)
        # 5. Update table.
        for r in ranges:
            strSql = f"update temp.{temp_layer} set kans_klasse = {r['class']} where kans between {r['from']} and {r['to']} "
            resSql = engine.execute(strSql)

        # 6. Upload new geoserver layer with same name as the temp table on geoserver with styling name equal to the classes range, eg 5 6, 7 8
        # 7. based on that table create a new probabilities table. similar as in the past. so from here and after procedure same? In the past there was already a probability table for each case. So I need to prepare a default one.
        # 7.1 create priorities table each time a reclassification of the probabilities happens
        strSql = f"create table temp.{temp_layer}_priorities as select id, geom, fid, kans_klasse, amsheep_klasse from ra2ce_2_0.{hazard_id};"
        resSql = engine.execute(strSql)

        # 7.2 alter table and add column priority
        strSql = f"alter table temp.{temp_layer}_priorities add priority INT;"
        resSql = engine.execute(strSql)

        # 7.3 update values in the priority column based on the priorities_matrix
        # This procedure is a copy paste of the ra2ceutils.py calccosts functionality

        for probability_index in range(len(priorities_matrix)):
            for impact_index in range(len(priorities_matrix[probability_index])):
                # print(probability_index, impact_index)
                strSql = f"update temp.{temp_layer}_priorities set priority = {priorities_matrix[probability_index][impact_index]} where kans_klasse = {probability_index +1 } and amsheep_klasse = {impact_index + 1}"
                resSql = engine.execute(strSql)

        resSql.close()

        # publish both layers on the geoserver. temp store.
        # temp_layer includes hazard_id_random_number
        # temp_layer_kans with style ra2ce_kans_default
        # temp_layer_priorities with style priorities? do I really nead a different style? afterall it's 1,2,3,4 also. I will use the default ra2ce. or the kans for the moment

        # connect to geoserver

        cat = Catalog(
            "{}/rest/".format(cf.get("GeoServer", "host")),
            username=cf.get("GeoServer", "user"),
            password=cf.get("GeoServer", "pass"),
        )
        datastore = cat.get_store("temp")

        ft_kans = cat.publish_featuretype(  # TODO: give a proper layer name that includes kans
            f"{temp_layer}",
            datastore,
            native_crs="EPSG:28992",
            jdbc_virtual_table="",
        )
        cat.save(ft_kans)

        ft_priorities = cat.publish_featuretype(
            f"{temp_layer}_priorities",
            datastore,
            native_crs="EPSG:28992",
            jdbc_virtual_table="",
        )
        cat.save(ft_priorities)

        kan_layer = cat.get_layer(f"{temp_layer}")
        priorities_layer = cat.get_layer(f"{temp_layer}_priorities")

        style = "ra2ce_kans_default"

        # set style and save layer
        # layer._set_default_style(style)
        kan_layer.default_style = style
        priorities_layer.default_style = style
        cat.save(kan_layer)
        cat.save(priorities_layer)

        # except Exception as e:
        #    res = {"errMsg": "ERROR: {}".format(e)}
        #    logging.info("""WPS [WpsRa2ceProvideHazardList]: ERROR = {}""".format(e))
        res = {
            "tempId": temp_id,
            "layers": [
                {
                    "baseUrl": cf.get("GeoServer", "wms_url"),
                    "layerName": temp_layer,
                },
                {
                    "baseUrl": cf.get("GeoServer", "wms_url"),
                    "layerName": f"{temp_layer}_priorities",
                },
            ],
        }

        response.outputs["output_json"].data = json.dumps(res)

        return response
