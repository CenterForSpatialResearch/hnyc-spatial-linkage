// initialize map    
var map = L
    .map('matches_map')
    .setView([40.7831, -73.9712], 12);

L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
	attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
	subdomains: 'abcd',
	maxZoom: 19
}).addTo(map);

L.svg().addTo(map);

// legend
function plotLegendCircle(legend, r, cx, cy, fill) {
    legend
        .append("circle")
        .attr("r", r)
        .attr("cx", cx)
        .attr("cy", cy)
        .style("fill", fill)
        .style("opacity", 0.4);
};
/*
function plotLegendCircle(legend, x1, y1, x2, y2, fill) {
    legend
        .append("line")
        .attr("x1", 0)
        .attr("y1", 70)
        .attr("x2", 20)
        .attr("y2", 70)
        .attr("stroke" , "black")
        .attr("stroke-width", 3);
};
*/
var legend = d3.select("body")
    .append("svg")
    .attr("id", "legend")
    .style("position", "absolute")
    .style("width", "15vw")
    .style("height", "175px")
    .style("background", "rgba(255, 255, 255, 0.9)")
    .style("z-index", "999")
    .style("transform", "translateX(85vw)");

plotLegendCircle(legend, 10, 10, 10, "red");
plotLegendCircle(legend, 10, 10, 40, "blue");

legend
    .append("line")
    .attr("x1", 0)
    .attr("y1", 70)
    .attr("x2", 20)
    .attr("y2", 70)
    .attr("stroke" , "grey")
    .attr("stroke-dasharray" , "4")
    .attr("stroke-width", 3);

legend
    .append("line")
    .attr("x1", 0)
    .attr("y1", 100)
    .attr("x2", 20)
    .attr("y2", 100)
    .attr("stroke", "black")
    .attr("stroke-dasharray" , "4")
    .attr("stroke-width", 3);

legend
    .append("line")
    .attr("x1", 0)
    .attr("y1", 130)
    .attr("x2", 20)
    .attr("y2", 130)
    .attr("stroke", "grey")
    .attr("stroke-width", 3);

legend
    .append("line")
    .attr("x1", 0)
    .attr("y1", 160)
    .attr("x2", 20)
    .attr("y2", 160)
    .attr("stroke", "black")
    .attr("stroke-width", 3);

legend
    .append("text")
    .attr("x", 30)
    .attr("y", 15)
    .text("CD Record");

legend
    .append("text")
    .attr("x", 30)
    .attr("y", 45)
    .text("Census Record");

legend
    .append("text")
    .attr("x", 30)
    .attr("y", 75)
    .text("True Negative");

legend
    .append("text")
    .attr("x", 30)
    .attr("y", 105)
    .text("False Positive");

legend
    .append("text")
    .attr("x", 30)
    .attr("y", 135)
    .text("False Negative");

legend
    .append("text")
    .attr("x", 30)
    .attr("y", 165)
    .text("True Positive");

// link to input
var selED = document.getElementById("selectED");
selED.addEventListener("change", function() {
    let ed = document.getElementById("selectED").value;
    plotData(ed, "ED");
});

var inputGraph = document.getElementById("selectGraph");
inputGraph.addEventListener("keyup", function(event) {

    // pressed enter
    if (event.keyCode === 13) {
        let graph = document.getElementById("selectGraph").value;
        plotData(graph, "graph");
    }
});

// plot data for that ED
function plotData(value, type) {

    d3.select("#matches_map").selectAll("svg").attr("pointer-events", "all");
    
    d3.csv("data/matched_viz_v3.csv", function(data) {
            
        if (type == "ED"){
            data = data.filter(function(d){return d.CENSUS_ENUMDIST == value;});
        } else if (type == "graph") {
            data = data.filter(function(d){ return parseInt(d.graph_ID) == value;});
        }

        var i = Math.floor(data.length / 2);

        map.flyTo( [data[i].LAT, data[i].LONG], zoom=18 );

        var svg = d3.select("#matches_map")
            .select("svg")
            .attr("id", "mapSVG")
            .selectAll("cd_circles")
            .data(data)
            .enter();
            
        // cd
        svg.append("circle")
            .attr("id", function(d) { return d.CD_ID })
            .attr("class", "cd")
            .attr("cx", function(d){ return map.latLngToLayerPoint([d.LAT, d.LONG]).x })
            .attr("cy", function(d){ return map.latLngToLayerPoint([d.LAT, d.LONG]).y })
            .attr("r", 20)
            .attr("visibility", "false")
            .attr("pointer-events", "visible")
            .style("fill", "#fd8b8b")
            .attr("stroke", false)
            .attr("fill-opacity", .4)
            .on("mouseover", function(d) {
                var className = "." + d.CD_ID;
                d3.selectAll(className)
                    .style("font-weight", "bold");

                table.row(className).scrollTo();

            })
            .on("mouseout", function(d) {
                var className = "." + d.CD_ID;
                d3.selectAll(className)
                    .style("font-weight", "normal");
            })
            .on("click", function(d) {
                var className = "." + d.CD_ID;
                d3.selectAll(className)
                    .style("background", "#f2f2f2");
            });

        // census
        svg.append("circle")
            .attr("id", function(d) { return d.CENSUS_ID; })
            .attr("class", "census")
            .attr("cx", function(d){ return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).x })
            .attr("cy", function(d){ return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).y })
            .attr("r", 20)
            .attr("visibility", "false")
            .attr("pointer-events", "visible")
            .style("fill", "#8ba4fd")
            .attr("stroke", false)
            .attr("fill-opacity", .4)
            .on("mouseover", function(d) {
                var className = "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("font-weight", "bold");

                table.row(className).scrollTo();

            })
            .on("mouseout", function(d) {
                var className = "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("font-weight", "normal");
            })
            .on("click", function(d) {
                var className = "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("background", "#f2f2f2");
            });

        // matches
        svg.append("line")
            .attr("id", function(d) { return d.CD_ID + d.CENSUS_ID; })
            .attr("class", "match")
            .attr("x1", function(d) { return map.latLngToLayerPoint([d.LAT, d.LONG]).x; })
            .attr("y1", function(d) { return map.latLngToLayerPoint([d.LAT, d.LONG]).y; })
            .attr("x2", function(d) { return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).x; })
            .attr("y2", function(d) { return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).y; })            
            .attr("visibility", "false")
            .attr("pointer-events", "visible")
            .attr("stroke", function(d) {
                if (d.selected_algo == 1) {
                    return "black";
                } else {
                    return "grey";
                }
            })
            .attr("stroke-dasharray", function(d) {
                if (d.selected_dist == 1) {
                    return "none";
                } else {
                    return "20";
                }
            })
            .attr("stroke-width", 3)
            .style("opacity", 0.5)
            .on("mouseover", function(d) {
                var className = "." + d.CD_ID + "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("font-weight", "bold");

                table.row(className).scrollTo();

            })
            .on("mouseout", function(d) {
                var className = "." + d.CD_ID + "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("font-weight", "normal");
            })
            .on("click", function(d) {
                var className = "." + d.CD_ID + "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("background", "#fafafa");
            });
            
        var tableWidth = document.getElementById("results").offsetWidth;
        var tableHeight = document.getElementById("results").offsetHeight;

        var table = $('#recordTable').DataTable({
            destroy: true,
            data: data,
            columns: [
                { "data" : "graph_ID", "title" : "Graph" },
                { "data" : "group_ID", "title" : "Group" },
                { "data" : "CENSUS_ENUMDIST", "title" : "ED" },
                { "data" : "CD_ID", "title" : "CD" },
                { "data" : "CENSUS_ID", "title" : "CENSUS" },
                { "data" : "CD_FIRST_NAME", "title" : "CD First Name" },
                { "data" : "CD_LAST_NAME", "title" : "CD Last Name" },
                { "data" : "MATCH_ADDR", "title" : "CD Addr" },
                { "data" : "CD_OCCUPATION", "title" : "CD Occupation" },
                { "data" : "CENSUS_NAMEFRSCLEAN", "title" : "Cen First Name" },
                { "data" : "CENSUS_NAMELASTB", "title" : "Cen Last Name" },
                { "data" : "CENSUS_MATCH_ADDR", "title" : "Cen Addr" },
                { "data" : "CENSUS_AGE", "title" : "Age" },
                { "data" : "CENSUS_OCCLABELB", "title" : "Cen Occupation" },
                { "data" : "confidence_score", "title" : "Confidence Score" },
                { "data" : "in_cluster", "title" : "Cluster" },
                { "data" : "spatial_weight", "title" : "Spatial Weight" },
                { "data" : "selected_algo", "title" : "Selected" },
                { "data" : "selected_dist", "title" : "True" },
                { "data" : "dist", "title" : "Distance" }
            ],
            initComplete: function () {
                this.api().columns().every( function () {
                    var column = this;
                    var select = $('<input type="text" placeholder="Search" size="3"/>')
                        .appendTo( $(column.header()) )
                        .on( 'keyup', function () {
                            var val = $.fn.dataTable.util.escapeRegex(
                                $(this).val()
                            );
        
                            column
                                .search( val )
                                .draw();
                        } );

                } );
            },
            "ordering" : false,
            scrollY: tableHeight * 0.7,
            scrollX: tableWidth * 0.9,
            scroller: true,
            "createdRow": function(row, data, dataIndex) {
                $(row).addClass(data.CD_ID);
                $(row).addClass(data.CENSUS_ID);
            },
            dom: 'Bfrtip',
            buttons: [ 'colvis' ]
        });

        // update location on zoom
        map.on("moveend", function() {
            d3.selectAll(".cd")
                .attr("cx", function(d){ return map.latLngToLayerPoint([d.LAT, d.LONG]).x })
                .attr("cy", function(d){ return map.latLngToLayerPoint([d.LAT, d.LONG]).y });

            d3.selectAll(".census")
                .attr("cx", function(d){ return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).x })
                .attr("cy", function(d){ return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).y });

            d3.selectAll(".match")
                .attr("x1", function(d) { return map.latLngToLayerPoint([d.LAT, d.LONG]).x; })
                .attr("y1", function(d) { return map.latLngToLayerPoint([d.LAT, d.LONG]).y; })
                .attr("x2", function(d) { return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).x; })
                .attr("y2", function(d) { return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).y; })            
            
        });
    });
};

$("body")
.on("click", "tr", function() {
    var cd =  this.classList[0];
    var census = this.classList[1];
    this.style.backgroundColor = "#fef3c7";

    d3.select("#mapSVG").selectAll("circle")
        .attr("visibility", function() {
            var visibility = d3.select(this).attr("visibility");
            if (visibility == "visible" ) { return "visible"; } else { return "hidden"; }
        })
    d3.select("#mapSVG").selectAll("line").attr("visibility", function() {
        var visibility = d3.select(this).attr("visibility");
        if (visibility == "visible" ) { return "visible"; } else { return "hidden"; }
        })

    d3.select('#' + cd).attr("visibility", "visible");
    d3.select('#' + census).attr("visibility", "visible")
    d3.select('#' + cd + census).attr("visibility", "visible");
    
})
.on("mouseover", "tr", function() { this.style.fontWeight = "bold"; })
.on("mouseout", "tr", function() { this.style.fontWeight = "normal" });

// clear selections
var clear = document.getElementById("clear");
clear.addEventListener("click", function() {
    var rows = document.getElementsByTagName("tr");

    for (var i = 0; i < rows.length; i++) {
        rows[i].style.backgroundColor = "#fff";
    }

    d3.select("#mapSVG").selectAll("*").attr("visibility", "visible").attr("pointer-events", "all");

});