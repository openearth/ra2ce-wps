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


class WpsRa2ceReclassifyRisks(Process):
    def __init__(self):

        inputs = [
            LiteralInput(
                "temp_layer", "temp_layer name in the database", data_type="string"
            ),
            LiteralInput("hazard_id", "hazard_id  in the database", data_type="string"),
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

        super(WpsRa2ceReclassifyRisks, self).__init__(
            self._handler,
            identifier="ra2ce_reclassify_risks",
            version="1.0",
            title="backend process for the RA2CE 2.0 version, reclassifies the risk layer",
            abstract="It reclassifies the risk layer based on the given priorities matrix",
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

        # 3. read from request hazard_id and priorities_matrix
        # temp_id = int(1000000 * time.time())
        temp_layer = request.inputs["temp_layer"][0].data
        hazard_id = request.inputs["hazard_id"][0].data
        priorities_matrix_str = request.inputs["priorities_matrix"][0].data
        priorities_matrix = json.loads(priorities_matrix_str)
        temp_id = temp_id = int(1000000 * time.time())
        print(temp_layer)
        strSql = f"create table temp.{temp_layer}_priorities_{temp_id} as select id, geom, fid, kans_klasse, amsheep_klasse, priority from temp.{temp_layer}_priorities;"
        resSql = engine.execute(strSql)
        print("temp_id", temp_id)
        # 7.3 update values in the priority column based on the priorities_matrix
        # This procedure is a copy paste of the ra2ceutils.py calccosts functionality

        for probability_index in range(len(priorities_matrix)):
            for impact_index in range(len(priorities_matrix[probability_index])):
                # print(probability_index, impact_index)
                strSql = f"update temp.{temp_layer}_priorities_{temp_id} set priority = {priorities_matrix[probability_index][impact_index]} where kans_klasse = {probability_index +1 } and amsheep_klasse = {impact_index + 1}"
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

        ft_priorities = cat.publish_featuretype(
            f"{temp_layer}_priorities_{temp_id}",
            datastore,
            native_crs="EPSG:28992",
            jdbc_virtual_table="",
        )
        cat.save(ft_priorities)

        priorities_layer = cat.get_layer(f"{temp_layer}_priorities_{temp_id}")

        style = "ra2ce_kans_default"

        # set style and save layer
        # layer._set_default_style(style)

        priorities_layer.default_style = style
        cat.save(priorities_layer)

        # except Exception as e:
        #    res = {"errMsg": "ERROR: {}".format(e)}
        #    logging.info("""WPS [WpsRa2ceProvideHazardList]: ERROR = {}""".format(e))
        res = {
            "id": f"{hazard_id}_risks",
            "name": f"{hazard_id} risks",
            "layer": f"ra2ce:{temp_layer}_priorities_{temp_id}",
        }

        response.outputs["output_json"].data = json.dumps(res)

        return response
