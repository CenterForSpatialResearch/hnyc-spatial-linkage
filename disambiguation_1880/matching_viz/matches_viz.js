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
var legend = d3.select("body")
    .append("svg")
    .attr("id", "legend")
    .style("position", "absolute")
    .style("width", "15vw")
    .style("height", "115px")
    .style("background", "rgba(255, 255, 255, 0.9)")
    .style("z-index", "999")
    .style("transform", "translateX(85vw)");

legend
    .append("circle")
    .attr("r", 10)
    .attr("cx", 10)
    .attr("cy", 10)
    .style("fill", "red")
    .style("opacity", 0.4);

legend
    .append("circle")
    .attr("r", 10)
    .attr("cx", 10)
    .attr("cy", 40)
    .style("fill", "blue")
    .style("opacity", 0.4);

legend
    .append("line")
    .attr("x1", 0)
    .attr("y1", 70)
    .attr("x2", 20)
    .attr("y2", 70)
    .attr("stroke" , "black")
    .attr("stroke-width", 3);

legend
    .append("line")
    .attr("x1", 0)
    .attr("y1", 100)
    .attr("x2", 20)
    .attr("y2", 100)
    .attr("stroke" , "grey")
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
    .text("Disambiguated Match");

legend
    .append("text")
    .attr("x", 30)
    .attr("y", 105)
    .text("ES Match");

// link to input
var form = document.getElementById("selectED");
form.addEventListener("change", function() {
    let ed = document.getElementById("selectED").value;
    plotData(ed);
});

// plot data for that ED
function plotData(ed) {

    d3.select("#matches_map").selectAll("svg").attr("pointer-events", "all");
        
    d3.csv("data/matched_viz.csv", function(data) {
        data = data.filter(function(d){return d.CENSUS_ENUMDIST == ed;});
        var i = Math.floor(data.length / 2);

        map.flyTo( [data[i].LAT, data[i].LONG], zoom=18 );

        var svg = d3.select("#matches_map")
            .select("svg")
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
            .style("fill", "#fd8b8b")
            .attr("stroke", false)
            .attr("fill-opacity", .4)
            .on("mouseover", function(d) {
                var className = "." + d.CD_ID;
                d3.selectAll(className)
                    .style("font-weight", "bold");

                var container = document.getElementById("results");
                var records = container.getElementsByClassName(d.CD_ID);
                if (records.length > 0) {
                    container.scrollTop = records[0].offsetTop;
                }
            })
            .on("mouseout", function(d) {
                var className = "." + d.CD_ID;
                d3.selectAll(className)
                    .style("font-weight", "normal");
            })
            .on("click", function(d) {
                var className = "." + d.CD_ID;
                d3.selectAll(className)
                    .style("background", "#fd8b8b");
            });

        // census
        svg.append("circle")
            .attr("id", function(d) { return d.CENSUS_ID; })
            .attr("class", "census")
            .attr("cx", function(d){ return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).x })
            .attr("cy", function(d){ return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).y })
            .attr("r", 20)
            .style("fill", "#8ba4fd")
            .attr("stroke", false)
            .attr("fill-opacity", .4)
            .on("mouseover", function(d) {
                var className = "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("font-weight", "bold");

                var container = document.getElementById("results");
                var records = container.getElementsByClassName(d.CENSUS_ID);
                if (records.length > 0) {
                    container.scrollTop = records[0].offsetTop;
                }
            })
            .on("mouseout", function(d) {
                var className = "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("font-weight", "normal");
            })
            .on("click", function(d) {
                var className = "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("background", "#8ba4fd");
            });

        // matches
        svg.append("line")
            .attr("id", function(d) { return d.CD_ID + d.CENSUS_ID; })
            .attr("class", "match")
            .attr("x1", function(d) { return map.latLngToLayerPoint([d.LAT, d.LONG]).x; })
            .attr("y1", function(d) { return map.latLngToLayerPoint([d.LAT, d.LONG]).y; })
            .attr("x2", function(d) { return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).x; })
            .attr("y2", function(d) { return map.latLngToLayerPoint([d.CENSUS_Y, d.CENSUS_X]).y; })            
            .attr("stroke", function(d) {
                if (d.selected == 1) {
                    return "black";
                } else {
                    return "grey";
                }
            })
            .attr("stroke-width", 3)
            .style("opacity", function(d) {
                if (d.selected == 1) {
                    return 1;
                } else {
                    return 0.5;
                }
            })
            .on("mouseover", function(d) {
                var className = "." + d.CD_ID + "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("font-weight", "bold");

                var container = document.getElementById("results");
                var records = document.querySelectorAll(className);
                if (records.length > 0) {
                    container.scrollTop = records[0].offsetTop;
                }
            })
            .on("mouseout", function(d) {
                var className = "." + d.CD_ID + "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("font-weight", "normal");
            })
            .on("click", function(d) {
                var className = "." + d.CD_ID + "." + d.CENSUS_ID;
                d3.selectAll(className)
                    .style("background", "#fef3c7");
            });
        
        // plot table
        var header = ['CD_ID', 'CENSUS_ID', 
                      'CD_FIRST_NAME', 'CD_LAST_NAME', 'MATCH_ADDR', 'CD_OCCUPATION',
                      'CENSUS_NAMEFRSCLEAN', 'CENSUS_NAMELASTB', 'CENSUS_MATCH_ADDR', 'CENSUS_AGE', 'CENSUS_OCCLABELB',
                      'confidence_score_x', 'in_cluster', 'spatial_weight', 'selected'];
        var headerLabs = ['CD', 'CENSUS',
                          'CD FR', 'CD LN', 'CD Add', 'CD Occ',
                          'Cen FN', 'Cen LN', 'Cen Add', 'Age', 'Cen Occ', 
                          'Confidence', 'Cluster', 'Weight', 'Select'];

        var table = document.createElement("table");
        var tr = table.insertRow(-1);

        for (var i = 0; i < headerLabs.length; i++) {
            var th = document.createElement("th");
            th.innerHTML = headerLabs[i];
            tr.appendChild(th);
        }

        for (var i = 0; i < data.length; i++) {

            tr = table.insertRow(-1);
            tr.classList.add(data[i]['CD_ID']);
            tr.classList.add(data[i]['CENSUS_ID']);

            for (var j = 0; j < header.length; j++) {
                var tabCell = tr.insertCell(-1);
                tabCell.innerHTML = data[i][header[j]];
            }
        }

        var results = document.getElementById("results");
        results.innerHTML = "";
        results.appendChild(table);
        
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
};

// clear selections
var clear = document.getElementById("clear");
clear.addEventListener("click", function() {
    var rows = document.getElementsByTagName("tr");

    for (var i = 0; i < rows.length; i++) {
        rows[i].style.backgroundColor = "#fff";
    }
});

// empty all
var empty = document.getElementById("empty");
empty.addEventListener("click", function() {
    d3.select("svg").selectAll("*").remove();

    document.getElementById("results").innerHTML = "";
    document.getElementById("selectED").selectedIndex = "none";
});