app = angular.module('graphApp', []);

app.controller('graphController', function ($scope, graphService) {
    $scope.viewResult = null;
    $scope.minLink = 2;

    $scope.load = function () {
        d3.select("#d3matrix").select("svg").remove();

        var loadDialog = document.getElementById("loading");
        loadDialog.showModal();
        var width = 960,
            height = 1000;

        var color = d3.scale.ordinal()
            .domain(["1", "2", "3", "4", "5"])
            .range(["#008000", "#00ff00", "#ffff00", "#FF0000", "#800000"]);

        var force = d3.layout.force()
            .gravity(.05)
            .distance(150)
            .charge(-100)
            .size([width, height]);

        var svg = d3.select("#d3matrix").append("svg")
            .attr("width", width)
            .attr("height", height);

        var nodes = [],
            links = [];


        dataset.nodes.forEach(function (node) {
            if(node.weightMax>=$scope.minLink) {
                nodes.push({name: node.name, group: node.group});
            }
        });
        dataset.links.forEach(function (link) {
            if(link.weight>=$scope.minLink) {
                var x=null,
                    y=null,
                    i=0;
                while ((x==null || y==null) && i<nodes.length){
                    if(link.source===nodes[i].name) x=i;
                    if(link.target===nodes[i].name) y=i;
                    i++;
                }
                links.push({source: x, target: y, weight: link.weight});
            }
        });

        force
            .nodes(nodes)
            .links(links)
            .start();

        var link = svg.selectAll(".link")
            .data(links)
            .enter().append("line")
            .attr("class", "link")
            .style("stroke-width", function (d) {
                return Math.sqrt(d.weight);
            });

        var node = svg.selectAll(".node")
            .data(nodes)
            .enter().append("g")
            .attr("class", "node")
            .call(force.drag);

        node.append("circle")
            .attr("r", "10")
            .style("fill", function (d) {
                return color(d.group);
            });

        node.append("text")
            .attr("class", "text1")
            .attr("dy", -12)
            .text(function (d) {
                return d.name
            });

        // node.append("text")
        //     .attr("class", "text2")
        //     .attr("dx", 12)
        //     .text(function (d) {
        //         return d.label;
        //     });

        force.on("tick", function () {
            link
                .attr("x1", function (d) {
                    return d.source.x;
                })
                .attr("y1", function (d) {
                    return d.source.y;
                })
                .attr("x2", function (d) {
                    return d.target.x;
                })
                .attr("y2", function (d) {
                    return d.target.y;
                });

            node.attr("transform", function (d) {
                return "translate(" + d.x + "," + d.y + ")";
            });

            //console.log("ciao");

        });


        $scope.viewResult = "active";
        loadDialog.close();
        $("html, body").animate({scrollTop: $("#resultScroll").offset().top}, 1000);
    };

    $scope.load();
});
app.factory('graphService', function ($http) {
    return {};
});