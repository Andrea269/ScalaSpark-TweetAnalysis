app = angular.module('graphApp', []);

app.controller('graphController', function ($scope, graphService) {
    $scope.viewResult = null;

    $scope.load = function () {
        d3.select("#d3matrix").select("svg").remove();

        var loadDialog = document.getElementById("loading");
        loadDialog.showModal();
        var width = 960,
            height = 600;

        var color = d3.scale.ordinal()
            .domain(["0", "1", "2", "3", "4"])
            .range(["#008000", "#00ff00", "#ffff00", "#FF0000", "#800000"]);

        var force = d3.layout.force()
            .gravity(.05)
            .distance(200)
            .charge(-100)
            .size([width, height]);

        var svg = d3.select("#d3matrix").append("svg")
            .attr("width", width)
            .attr("height", height);

        var nodes = dataset.nodes.slice(),
            links = [],
            bilinks = [];

        dataset.links.forEach(function (link) {
            var s = nodes[link.source],
                t = nodes[link.target],
                i = {}; // intermediate node
            nodes.push(i);
            links.push({source: s, target: i}, {source: i, target: t});
            bilinks.push([s, i, t]);
        });

        force
            .nodes(dataset.nodes)
            .links(dataset.links)
            .start();

        var link = svg.selectAll(".link")
            .data(dataset.links)
            .enter().append("line")
            .attr("class", "link")
            .style("stroke-width", function (d) {
                return Math.sqrt(d.weight);
            });

        var node = svg.selectAll(".node")
            .data(dataset.nodes)
            .enter().append("g")
            .attr("class", "node")
            .call(force.drag);

        node.append("circle")
            .attr("r", "10")
            .style("fill", function (d) {
                return color(d.group);
            });

        node.append("text")
            .attr("dx", 12)
            .attr("dy", ".35em")
            .text(function (d) {
                return d.name
            });

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
        });

        $scope.viewResult = "active";
        loadDialog.close();
    };

    $scope.load();
});
app.factory('graphService', function ($http) {
    return {};
});