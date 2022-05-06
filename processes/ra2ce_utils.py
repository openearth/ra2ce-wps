# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2018 Deltares
#       Gerrit Hendriksen
#       gerrit.hendriksen@deltares.nl
#       Ioanna Micha
#       ioanna.micha@deltares.nl
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

import os
import json
import configparser
from sqlalchemy import create_engine, exc
import logging
import numpy as np
from geoserver.catalog import Catalog
import time
from os.path import join, dirname, realpath, abspath


def calccosts(cf, layer_name, json_matrix):
    # randomnr = np.random.randint(0, 1000000000)
    new_layer = "classroads_{}".format(int(1000000 * time.time()))

    # calccost function calculates recalcutes the visualisation caterogies
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
        pool_pre_ping=True,
    )

    try:
        strSql = """create table temp.{new_layer} as select shape, societal_class, repair_class, 0 as visclass from ra2ce.{layer_name}_prioriteiten;""".format(
            new_layer=new_layer, layer_name=layer_name
        )
        ressql = engine.execute(strSql)
    except exc.DBAPIError as e:
        # an exception is raised, Connection is invalidated.
        if e.connection_invalidated:
            logging.info("Connection was invalidated!")

    data = json.loads(json_matrix)

    values = data["values"]
    for societalIndex in range(len(values)):
        for repairIndex in range(len(values[societalIndex])):
            strSql = """update temp.{new_layer} set visclass = {val}
                            where societal_class = {s} and repair_class = {r};""".format(
                new_layer=new_layer,
                val=values[societalIndex][repairIndex],
                s=societalIndex + 1,
                r=repairIndex + 1,
            )
            ressql = engine.execute(strSql)
        ressql.close()

    # publish the layer in geoserver
    cat = Catalog(
        "{}/rest/".format(cf.get("GeoServer", "host")),
        username=cf.get("GeoServer", "user"),
        password=cf.get("GeoServer", "pass"),
    )
    datastore = cat.get_store("temp")
    ft = cat.publish_featuretype(
        new_layer, datastore, native_crs="EPSG:28992", jdbc_virtual_table=""
    )
    cat.save(ft)
    layer = cat.get_layer(new_layer)

    # check if shapefile is line or point
    if layer_name in [
        "bruggen",
        "opdrijvenlichtgewichtconstructies",
        "opdrijventunnels",
    ]:
        style = "ra2ce_punt"
    else:
        style = "ra2ce"

    # set style and save layer
    # layer._set_default_style(style)
    layer.default_style = style
    cat.save(layer)

    res = writeOutput(cf=cf, wmslayer="ra2ce:{}".format(new_layer), defstyle=style)
    # time.sleep(2)
    return res


def select_from_db(cf, LayerName):
    # get layer/table from postgresql
    layer_operator_costs = "ra2ce:" + LayerName + "_herstelkosten"
    layer_societal_costs = "ra2ce:" + LayerName + "_stremmingskosten"

    # TO DO: input for different legends
    res_operator = writeOutput(cf=cf, wmslayer=layer_operator_costs, defstyle="ra2ce")
    res_societal = writeOutput(cf=cf, wmslayer=layer_societal_costs, defstyle="ra2ce")

    return res_operator, res_societal


# Read default configuration from file
def read_config():

    configf = join(dirname(realpath(__file__)), "ra2ce.txt")
    cf = configparser.RawConfigParser()
    cf.read(configf)
    return cf


# Write output
def writeOutput(cf, wmslayer, defstyle="ri2de"):
    res = dict()
    res["baseUrl"] = cf.get("GeoServer", "wms_url")
    res["layerName"] = wmslayer
    res["style"] = defstyle
    return json.dumps(res)


# Delete all tables in the temp schema in the db
def deleteTablesSchemaDB(cf):
    # does not work in python but works in postgresql
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
        pool_pre_ping=True,
    )

    sql_delete = """
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'temp') LOOP
                        EXECUTE 'DROP TABLE IF EXISTS temp.' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
                """
    engine.execute(sql_delete)
    print("All tables in schema 'temp' are deleted.")


# Delete all temporary layers in geoserver
def deleteTempLayersGeoserver(cf):
    cat = Catalog(
        "{}/rest".format(cf.get("GeoServer", "host")),
        username=cf.get("GeoServer", "user"),
        password=cf.get("GeoServer", "pass"),
    )
    my_resources = cat.get_resources(stores=["temp"], workspaces=["ra2ce"])

    counter = 0
    for l in my_resources:
        name = l.name
        layer = cat.get_layer(name)
        # check if layer exists
        if not layer == None:
            logging.info("deleted:", name)
            cat.delete(layer)
            counter += 1

    return counter
