app = angular.module('bubbleChartApp', []);

app.controller('bubbleChartController', function ($scope, bubbleChartService) {
    $scope.viewResult = null;
    $scope.minBubble = 1;
    $scope.errorInput = null;

    $scope.sendInfo = function () {
        var numBubble = document.getElementById("bubbleThreshold").value;
        if (numBubble > 0) {
            $scope.errorInput = null;
            $scope.minBubble = numBubble;
            $scope.load();
        } else {
            $scope.errorInput = "Inserire un numero maggiore di 0";
        }
    };

    $scope.load = function () {
        d3.select("#d3matrix").select("svg").remove();
        var loadDialog = document.getElementById("loading");
        loadDialog.showModal();

        var temp = [];
        dataset.children.forEach(function (node) {
            if (node.count >= $scope.minBubble) {
                temp.push({name: node.name, count: node.count});
            }
        });
        var data= {children: temp};

        if (temp.length>0) {
            var diameter = 800;
            var color = d3.scaleOrdinal(d3.schemeCategory20);

            var bubble = d3.pack(data)
                .size([diameter, diameter])
                .padding(1.5);

            var svg = d3.select("#d3matrix").append("svg")
                .attr("width", diameter)
                .attr("height", diameter)
                .attr("class", "bubble");

            var nodes = d3.hierarchy(data)
                .sum(function (d) {
                    return d.count;
                });

            var node = svg.selectAll(".node")
                .data(bubble(nodes).descendants())
                .enter()
                .filter(function (d) {
                    return !d.children
                })
                .append("g")
                .attr("class", "node")
                .attr("transform", function (d) {
                    return "translate(" + d.x + "," + d.y + ")";
                });

            node.append("title")
                .text(function (d) {
                    return d.name + ": " + d.count;
                });

            node.append("circle")
                .attr("r", function (d) {
                    return d.r;
                })
                .style("fill", function (d, i) {
                    return color(i);
                });

            node.append("text")
                .attr("dy", ".2em")
                .style("text-anchor", "middle")
                .text(function (d) {
                    return d.data.name.substring(0, d.r / 4);
                })
                .attr("font-family", "sans-serif")
                .attr("font-size", function (d) {
                    return d.r / 5;
                })
                .attr("fill", "white");

            node.append("text")
                .attr("dy", "1.3em")
                .style("text-anchor", "middle")
                .text(function (d) {
                    return d.data.count;
                })
                .attr("font-family", "Gill Sans", "Gill Sans MT")
                .attr("font-size", function (d) {
                    return d.r / 5;
                })
                .attr("fill", "white");

            d3.select(self.frameElement)
                .style("height", diameter + "px");
            $scope.viewResult = "active";
        }else{
            $scope.viewResult = null;
        }
        loadDialog.close();
    };

    $scope.load();
});
app.factory('bubbleChartService', function ($http) {
    return {};
});