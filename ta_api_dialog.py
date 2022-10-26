# -*- coding: utf-8 -*-
"""
/***************************************************************************
 TripAdvisorDialog
                                 A QGIS plugin
 this plugin loads reviews from tripadvisor via scraping and geotags them
 Generated by Plugin Builder: http://g-sherman.github.io/Qgis-Plugin-Builder/
                             -------------------
        begin                : 2022-10-25
        git sha              : $Format:%H$
        copyright            : (C) 2022 by Arka
        email                : arkaprava.mail@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

import json
import os
import sys
from operator import itemgetter

sys.path.append(os.path.join(os.path.dirname(__file__)))

from ta_scraper import TAapi

from qgis.PyQt import uic
from qgis.PyQt import QtWidgets
from qgis.PyQt.QtCore import QThread, QVariant
from qgis.PyQt.QtWidgets import QMessageBox
from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY, QgsProject, QgsMarkerSymbol, QgsField

from PyQt5.QtWebKitWidgets import QWebView
from PyQt5.QtWebKit import QWebSettings

QWebSettings.globalSettings().setAttribute(QWebSettings.DeveloperExtrasEnabled, True)

# This loads your .ui file so that PyQt can populate your plugin with the elements from Qt Designer
FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'ta_api_dialog_base.ui'))

with open(os.path.join(os.path.dirname(__file__), 'template.html'), 'r') as f:
    __HTML_TEMPLATE__ = f.read()



class TripAdvisorDialog(QtWidgets.QDialog, FORM_CLASS):
    def __init__(self, parent=None):
        super(TripAdvisorDialog, self).__init__(parent)
        self.setupUi(self)

        # set download in progress flag as false
        self.isDownloadInProgress = False

        # set logbox empty
        self.logBox.setPlainText("")

        # set progress bar to zero
        self.progressBar.setValue(0)

        # disable stop button
        self.stopButton.setEnabled(False)

        self.startButton.clicked.connect(self._start_download_thread)
        self.stopButton.clicked.connect(self._stop_download_thread)
        self.closeWindows.clicked.connect(self._close_browser_windows)
        self.removeLayers.clicked.connect(self._remove_layers)

        self.elem_config_map = {
            "API_KEY": self.apiKey,
            "KEYWORD": self.keyword,
            "RADIUS": self.radius,
            "LAT": self.lat,
            "LNG": self.lng,
            "DBNAME": self.dbName,
            "TABLENAME": self.tableName,
            "DOWNLOAD_IMAGES": self.downloadImages
        }

        self.configFilePath = os.path.join(os.path.dirname(__file__), "ui.dat")

        # load saved input
        self._load_prev_input()

        # connect to input saver
        self.rejected.connect(self._cleanup)

    def _remove_layers(self):
        try:
            QgsProject.instance().removeMapLayers([self.markerLayer.id(), self.boundaryLayer.id()])
            QgsProject.instance().refreshAllLayers()
        except:
            pass

    def _close_browser_windows(self):
        if hasattr(self, 'webViews'):
            for webView in self.webViews:
                try:
                    webView.close()
                except:
                    pass

    def _cleanup(self):
        # save inputs
        self._save_input()

        # clean vector layer
        self._remove_layers()

        # close open browser windows
        self._close_browser_windows()

    def _save_input(self):
        try:
            f = open(self.configFilePath, 'w')
        except:
            return
        
        l = list()

        for key, val in self.elem_config_map.items():
            if key == 'DOWNLOAD_IMAGES':
                l.append(f"{key}={'true' if val.isChecked() else 'false'}")
            else:
                l.append(f"{key}={val.text()}")

        f.write('\n'.join(l))
        f.close()

    def _load_prev_input(self):
        if os.path.exists(self.configFilePath):
            # load configurations from configfile
            try:
                f = open(self.configFilePath)
            except:
                self.logBox.append("Error: could not load from config file.")
                return

            for line in f.readlines():
                key, val = line.strip('\n').split("=")
                elem = self.elem_config_map[key]

                if key == 'DOWNLOAD_IMAGES':
                    elem.setChecked(val == "true")
                else:    
                    elem.setText(val)

            f.close()
            return

    def _start_download_thread(self):
        # starts download thread

        self.progressBar.setValue(0)

        def number_error(elem, field):
            QMessageBox.warning(self, "Error", f"Enter valid number for {field}")
            elem.setFocus()
            elem.selectAll()

        def lat_error(elem):
            QMessageBox.warning(self, "Error", "latitude must lie between -90 and 90 degrees")
            elem.setFocus()
            elem.selectAll()

        def long_error(elem):
            QMessageBox.warning(self, "Error", "longitude must lie between -180 and 180 degrees")
            elem.setFocus()
            elem.selectAll()

        if not self.isDownloadInProgress:
            apiKey = self.apiKey.text()
            keyword = self.keyword.text()
            radius = self.radius.text()
            lat = self.lat.text()
            lng = self.lng.text()
            dbName = self.dbName.text()
            tableName = self.tableName.text()

            if len(apiKey) == 0:
                QMessageBox.warning(self, "Error", "api key can't be empty")
                self.apiKey.setFocus()

            if len(keyword) == 0:
                QMessageBox.warning(self, "Error", "keyword is empty")
                self.keyword.setFocus()

            if len(dbName) == 0:
                QMessageBox.warning(self, "Error", "database name is empty")
                self.dbName.setFocus()

            if len(tableName) == 0:
                QMessageBox.warning(self, "Error", "table name is empty")
                self.tableName.setFocus()                


            try:
                radius = int(radius)
            except:
                number_error(self.radius, "radius")

            try:
                lat = float(lat)
            except:
                number_error(self.lat, "latitude")

            try:
                lng = float(lng)
            except:
                number_error(self.lng, "longitude")

            if not (-90 <= lat <= 90):
                lat_error(self.lat)

            if not (-180 <= lng <= 180):
                long_error(self.lng)

            if set(['radius', 'lat', 'lng']) < set(locals()) and \
                -180 <= lng <= 180 and -90 <= lat <= 90:
                # no error => set download in progress
                self.isDownloadInProgress = True
                self.startButton.setEnabled(False)
                self.stopButton.setEnabled(True)

                # clear log
                self.logBox.clear()

                # create thread handler
                self.thread = QThread()

                # create worker
                self.worker = TAapi(keyword, lat, lng, radius, apiKey, dbName, tableName)
                self.worker.moveToThread(self.thread)

                self.worker.addMessage.connect(self._message_from_worker)
                self.worker.addError.connect(self._error_from_worker)
                self.worker.progress.connect(self._progress_from_worker)
                self.worker.total.connect(self._total_from_worker)
                self.worker.apiUsage.connect(self._api_usage_from_worker)

                self.thread.started.connect(self.worker.run)
                self.worker.finished.connect(self.thread.quit)
                self.worker.finished.connect(self.worker.deleteLater)
                self.thread.finished.connect(self.thread.deleteLater)

                # start thread
                self.thread.start()

                def worker_finished(data):
                    self.logBox.append(f"worker finished. {len(data)} places with reviews collected.")

                    self.startButton.setEnabled(True)    
                    self.stopButton.setEnabled(False)

                    self.isDownloadInProgress = False
                    self.progressBar.setValue(self.progressBar.maximum())  

                    center = {
                        'lat': lat,
                        'lng': lng
                    }
                    self._draw_layers(center, radius, data)

                self.worker.finished.connect(worker_finished)
            else:
                QMessageBox.warning(self, "Error", "Can not download without appropriate data!")
        else:
            pass

    def _draw_layers(self, center, radius, data):
        self.logBox.append("drawing vector layers...")
        lat, lng = itemgetter('lat', 'lng')(center)

        # create boundary layer
        self.boundaryLayer = QgsVectorLayer("Point?crs=epsg:4326", "tapi boundary", "memory")
        self.boundaryProvider = self.boundaryLayer.dataProvider()
        self.boundaryLayer.startEditing()

        # define symbol to be a boundary
        symbol = QgsMarkerSymbol.createSimple({
            'name': 'circle', 
            'color': '255, 255, 255, 0',
            'size': str(2 * radius),
            'size_unit': 'RenderMetersInMapUnits',
            'outline_color': '35,35,35,255', 
            'outline_style': 'solid', 
            'outline_width': '30',
            'outline_width_unit': 'RenderMetersInMapUnits'
        })
        
        self.boundaryLayer.renderer().setSymbol(symbol)

        # draw circular boundary
        boundary = QgsFeature()
        boundary.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(lng, lat)))
        self.boundaryProvider.addFeatures([boundary])

        self.boundaryLayer.commitChanges()
        QgsProject.instance().addMapLayer(self.boundaryLayer)

        # create marker layer
        self.markerLayer = QgsVectorLayer("Point?crs=epsg:4326", "tapi markers", "memory")
        self.markerProvider = self.markerLayer.dataProvider()
        self.markerLayer.startEditing()

        self.markerProvider.addAttributes([
            QgsField('name', QVariant.String),
            QgsField('latitude', QVariant.Double),
            QgsField('longitude', QVariant.Double),
            QgsField('url', QVariant.String),
            QgsField('reviews', QVariant.List),
            QgsField('mode', QVariant.String),
        ])

        self.logBox.append(f"adding {len(data)} markers")

        for row in data:
            name, url, reviews, mode = itemgetter('name', 'url', 'reviews', 'mode')(row)
            lat, lng = itemgetter('lat', 'lng')(row['coords'])
            marker = QgsFeature()
            marker.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(lng, lat)))
            marker.setAttributes([
                name,
                lat,
                lng,
                url,
                reviews,
                mode
            ])
            self.markerProvider.addFeatures([marker])

        self.markerLayer.commitChanges()
        QgsProject.instance().addMapLayer(self.markerLayer)

        # add selection handler
        self.markerLayer.selectionChanged.connect(self._handle_feature_selection)
        self.webViews = []

    def _handle_feature_selection(self):
        selFeatures = self.markerLayer.selectedFeatures()
        if len(selFeatures) > 0:
            for feature in selFeatures:
                attrs = feature.attributes()
                name, lat, lng, url, reviews, mode = attrs

                # draw popup on web view or use native qt dialog
                self._open_web_view(name, lat, lng, url, reviews, mode)

    def _open_web_view(self, name, lat, lng, url, reviews, mode):
        webView = QWebView()
        self.webViews.append(webView)

        self.logBox.append(f"loading page for: {name}")

        reviewsString = json.dumps(reviews)

        webView.setHtml(__HTML_TEMPLATE__.format(name, name, "Places To Go" if mode == "place" else "Things To Do", reviewsString))
        webView.show()

    def _stop_download_thread(self):
        self.worker.stop()

    def _message_from_worker(self, message):
        self.logBox.append(message)

    def _error_from_worker(self, message):
        QMessageBox.warning(self, "Error", message)

    def _progress_from_worker(self, progress):
        self.progressBar.setValue(progress)

    def _total_from_worker(self, total):
        self.progressBar.setMaximum(int(total))

    def _api_usage_from_worker(self, usage, bill):
        self.apiUsage.setText(f"{usage} times - $ {bill}")
        self.apiUsage.repaint()
