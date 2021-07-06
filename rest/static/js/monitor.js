//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

function MonitorCtrl($scope, $http, $routeParams, $log, $sce, expvar) {
    $scope.indexNames = [];
    $scope.currIndexName = null;
    $scope.monitoredIndexes = {};

    var tv = 1000;
    var top = "/bleve/";
    var updateInterval = null;
    var isoDateFormatter = function(x) { return ISODateString(new Date(x*1000)); };
    var byteSizeFormatter = function(y) { return Humanize.fileSize(y); };
    var nanosecondFormatter = function(y) {
        if (y > 1000000) {
            return Humanize.toFixed(y/1000000) + " ms";
        } else {
            return y + " ns";
        }
    };
    var intFormatter = function(y) {
        return Humanize.compactInteger(y);
    };

    var updateData = function() {
        expvar.pollExpvar();

        for (var i in $scope.metrics) {
            category = $scope.metrics[i];
            for (var k in category.metrics) {
                redrawMetric(category.metrics[k]);
            }
        }

        $scope.indexNames = expvar.getDynamicDataKeys("indexes");

        for (var indexName in $scope.monitoredIndexes) {
            if (indexName != $scope.currIndexName) {
                removeIndex($scope.monitoredIndexes[indexName]);
                delete $scope.monitoredIndexes[indexName];
            }
        }

        var idx = $scope.monitoredIndexes[$scope.currIndexName];
        if (!idx && $scope.currIndexName) {
            idx = $scope.monitoredIndexes[$scope.currIndexName] =
                monitorIndex($scope.currIndexName);
        }
        if (idx) {
            for(var j in idx.metrics) {
                redrawMetric(idx.metrics[j]);
            }
        }
    };

    function monitorIndex(name) {
        $log.info("start monitoring index: " + name);
        idx = {
            name: name,
            metrics: [
                {
                    name: name+"updates",
                    display: "Updates",
                    path: top + "indexes/" + name  + "/index/updates",
                    type: "rate",
                    color: "steelblue",
                    yaxis: Rickshaw.Fixtures.Number.formatKMBT,
                    xformatter: isoDateFormatter,
                    yformatter: intFormatter
                },
                {
                    name: name+"deletes",
                    display: "Deletes",
                    path: top + "indexes/"+ name  +"/index/deletes",
                    type: "rate",
                    color: "steelblue",
                    yaxis: Rickshaw.Fixtures.Number.formatKMBT,
                    xformatter: isoDateFormatter,
                    yformatter: intFormatter
                },
                {
                    name: name+"indexanalysistime",
                    display: "Analysis/Index Time",
                    series: [
                        {
                            display: "Index",
                            name: name+"indextime",
                            path: top + "indexes/"+ name  +"/index/index_time",
                            type: "rate",
                            color: "red",
                        },
                        {
                            display: "Analysis",
                            name: name+"analysistime",
                            path: top + "indexes/" + name  + "/index/analysis_time",
                            type: "rate",
                            color: "green",
                        }
                    ],
                    yaxis: Rickshaw.Fixtures.Number.formatKMBT,
                    xformatter: isoDateFormatter,
                    yformatter: nanosecondFormatter,
                    legend: true
                },
                {
                    name: name+"searches",
                    display: "Searches",
                    path: top + "indexes/"+ name  +"/searches",
                    type: "rate",
                    color: "steelblue",
                    yaxis: Rickshaw.Fixtures.Number.formatKMBT,
                    xformatter: isoDateFormatter,
                    yformatter: intFormatter
                },
                {
                    name: name+"searchtime",
                    display: "Search Time",
                    path: top + "indexes/"+ name  +"/search_time",
                    type: "rate",
                    color: "steelblue",
                    yaxis: Rickshaw.Fixtures.Number.formatKMBT,
                    xformatter: isoDateFormatter,
                    yformatter: nanosecondFormatter
                },
            ]
        };

        indexDivName = "index" + idify(name);
        indexPanel =
            '<div id="panel'+idify(name)+'" class="panel panel-default">' +
            ' <div class="panel-heading">' + name + '</div>' +
            ' <div id="' + indexDivName + '" class="panel-body collapse in"></div>' +
            '</div>';
        $(indexPanel).insertBefore('#indexchartend');

        for (var i in idx.metrics) {
            metric = idx.metrics[i];
            mname = idify(metric.name);

            divContent =
                '<h5 id="header' +mname+'">' + metric.display + '</h5>' +
                '<div id="'+mname+'"></div>';

            $(divContent).appendTo('#'+indexDivName);

            if (metric.legend) {
                legendDivName = "legend" + idify(name);
                legendContent = '<div id="' + legendDivName + '" class="legend"></div>';
                $(legendContent).appendTo('#'+indexDivName);
            }

            // ask the expvar service to track this metric for us
            if (metric.path !== undefined) {
                expvar.addMetric(metric.name, metric.path);
            } else if (metric.series !== undefined) {
                for (var si in metric.series) {
                    sm = metric.series[si];

                    var swatch = document.createElement('div');
                    swatch.className = 'swatch';
                    swatch.style.backgroundColor = sm.color;
                    $(swatch).appendTo('#'+legendDivName);

                    var label = document.createElement('div');
                    label.className = 'label';
                    label.innerHTML = sm.display;
                    $(label).appendTo('#'+legendDivName);

                    expvar.addMetric(sm.name, sm.path);
                }
            }

            // build chart
            addGraph(metric);
        }

        return idx;
    }

    function removeIndex(index) {
        if (index) {
            for (var i in index.metrics) {
                metric = index.metrics[i];
                mname = idify(metric.name);
                expvar.removeMetric(metric.name);
                $("#panel" + mname).remove();
                $("#header" + mname).remove();
                $("#" + mname).remove();
            }
            $("#panel" + idify(index.name)).remove();
        }
    }

    function redrawMetric(metric) {
        graph = metric.graph;
        if (!graph || !graph.series) {
            return;
        }

        if (metric.series !== undefined) {
            var seriesData = [];
            for (var si in metric.series) {
                sm = metric.series[si];
                if (sm.type == "value") {
                    currentValue = expvar.getMetricCurrentValue(sm.name);
                    seriesData.push(currentValue);
                } else if (sm.type == "rate") {
                    currentRate = expvar.getMetricCurrentRate(sm.name);
                    seriesData.push(currentRate);
                    $log.info("name: " + sm.name + " " + currentRate);
                    $log.info(seriesData);
                }
            }
            graph.series.addData(seriesData);
        } else {
            var d = {};
            if (metric.type == "value") {
                currentValue = expvar.getMetricCurrentValue(metric.name);
                d[metric.name]= currentValue;
            } else if (metric.type == "rate") {
                currentRate = expvar.getMetricCurrentRate(metric.name);
                d[metric.name]= currentRate;
            }
            graph.series.addData(d);
        }

        // redraw
        graph.render();
    }

    $scope.metrics = {
        "memory": {
            "display": "Memory",
            metrics: [
                {
                    name: "alloc",
                    display: "Memory Allocated",
                    path: "/memstats/Alloc",
                    type: "value",
                    color: "steelblue",
                    yaxis: Rickshaw.Fixtures.Number.formatKMBT,
                    xformatter: isoDateFormatter,
                    yformatter: byteSizeFormatter
                },
                {
                    name: "pauseTotalNs",
                    display: "Garbage Collection Time",
                    path: "/memstats/PauseTotalNs",
                    type: "rate",
                    color: "steelblue",
                    yaxis: Rickshaw.Fixtures.Number.formatKMBT,
                    xformatter: isoDateFormatter,
                    yformatter: nanosecondFormatter,
                }
            ]
        }
    };

    expvar.addDynamicDataPath("indexes", top + "indexes");

    for (var categoryName in $scope.metrics) {
        category = $scope.metrics[categoryName];

        divName = "cat" + idify(categoryName);
        panel =
            '<div class="panel panel-default">' +
            ' <div class="panel-heading">' + category.display + '</div>' +
            ' <div id="' + divName + '" class="panel-body collapse in">' +
            ' </div>' +
            '</div>';
        $(panel).insertBefore('#chartend');

        for (var i in category.metrics) {
            metric = category.metrics[i];
            mname = idify(metric.name);

            divContent = '<h5>'+metric.display+'</h5><div id="'+mname+'"></div>';

            $(divContent).appendTo("#"+divName);

            // ask the expvar service to track this metric for us
            expvar.addMetric(metric.name, metric.path);

            // build chart
            addGraph(metric);
        }
    }

    function addGraph(metric) {
        mname = idify(metric.name);

        var seriesData = [];
        if (metric.series !== undefined) {
            for (var si in metric.series) {
                sm = metric.series[si];
                seriesData.push({
                    name: sm.name,
                    color: sm.color
                });
            }
        } else {
            seriesData.push({
                name: metric.name,
                color: metric.color
            });
        }

        $log.info("seriesdata");
        $log.info(seriesData);
        $log.info("seriesdataend");

        var graph = new Rickshaw.Graph({
            element: document.querySelector("#"+mname),
            width: "800",
            height: "75",
            renderer: "area",
            series: new Rickshaw.Series.FixedDuration(seriesData,
            undefined,
            {
                timeInterval: tv,
                maxDataPoints: 600,
                timeBase: new Date().getTime() / 1000
            })
        });

        // store the graph object inside the metric
        metric.graph = graph;

        // y-axis ticks
        if (metric.yaxis) {
            var yAxis = new Rickshaw.Graph.Axis.Y({
                graph: graph,
                tickFormat: metric.yaxis,
            });

            yAxis.render();
        }

        var xAxis = new Rickshaw.Graph.Axis.X({
            graph: graph,
            pixelsPerTick: 1000
        });
        xAxis.render();

        // set up the hover
        var hoverDetail = new Rickshaw.Graph.HoverDetail( {
            graph: graph,
            formatter: function(series, x, y, formattedX, formattedY, d) {
                var date = '<span class="x">' + formattedX + '</span>';
                var content =  formattedY + '<br>' + date;
                return content;
            }
        });

        if (metric.xformatter) {
            hoverDetail.xFormatter = metric.xformatter;
        }
        if (metric.yformatter) {
            hoverDetail.yFormatter = metric.yformatter;
        }

        // render it
        graph.render();
    }

    // setup data updates
    updateInterval = setInterval(updateData, tv);
    $scope.$on("$destroy", function(){
        clearInterval(updateInterval);
    });

    function ISODateString(d){
        function pad(n) { return n<10 ? '0'+n : n; }
        return d.getUTCFullYear()+'-' +
            pad(d.getUTCMonth()+1)+'-' +
            pad(d.getUTCDate())+'T' +
            pad(d.getUTCHours())+':' +
            pad(d.getUTCMinutes())+':' +
            pad(d.getUTCSeconds())+'Z';
    }

    function idify(s) {
        return s.replace(/\./g, '-');
    }
}
