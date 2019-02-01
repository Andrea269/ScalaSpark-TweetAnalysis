app = angular.module('graphApp', []);


app.controller('graphController', function ($scope, graphService) {
    $scope.viewResult = null;

    $scope.load = function () {
        d3.select("#d3matrix").select("svg").remove();

        var loadDialog = document.getElementById("loading");
        loadDialog.showModal();
        var width = 960,
            height = 600;

        var svg = d3.select("#d3matrix").append("svg")
            .attr("width", width)
            .attr("height", height);

        var force = d3.layout.force()
            .gravity(.05)
            .distance(100)
            .charge(-100)
            .size([width, height]);

        d3.json("libraries&components/datiGraph.json", function (json) {
            force
                .nodes(json.nodes)
                .links(json.links)
                .start();

            var link = svg.selectAll(".link")
                .data(json.links)
                .enter().append("line")
                .attr("class", "link")
                .style("stroke-width", function (d) {
                    return Math.sqrt(d.weight);
                });

            var node = svg.selectAll(".node")
                .data(json.nodes)
                .enter().append("g")
                .attr("class", "node")
                .call(force.drag);

            node.append("circle")
                .attr("r", "5");

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
        });
        $scope.viewResult = "active";
        loadDialog.close();
    };

    $scope.load();
});
app.factory('graphService', function ($http) {
    return {};
});