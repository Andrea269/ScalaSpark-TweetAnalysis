app = angular.module('demoApp', []);


app.controller('demoController', function ($scope, demoService) {


    $scope.graph = null;
    $scope.bubbleChart = null;
    $scope.viewResult = null;


    $scope.load = function () {
        $scope.activeGraph();
    };

    $scope.resetView = function () {
        // $scope.fileCreate = null;
        $scope.errorInput = null;
        $scope.bubbleChart = null;
        $scope.graph = null;
    };

    $scope.activeGraph = function () {
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

        d3.json("datiGraph.json", function (json) {
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
        $scope.graph = "active";
        $scope.bubbleChart = null;
        $scope.viewResult = "active";
        loadDialog.close();
    };

    $scope.activeBubbleChart = function () {
        d3.select("#d3matrix").select("svg").remove();
        var loadDialog = document.getElementById("loading");
        loadDialog.showModal();
        var width = 960,
            height = 60;




        var dataset = {
            "children": [{"Name":"Olives","Count":4319},
                {"Name":"Tea","Count":4159},
                {"Name":"Mashed Potatoes","Count":2583},
                {"Name":"Boiled Potatoes","Count":2074},
                {"Name":"Milk","Count":1894},
                {"Name":"Chicken Salad","Count":1809},
                {"Name":"Vanilla Ice Cream","Count":1713},
                {"Name":"Cocoa","Count":1636},
                {"Name":"Lettuce Salad","Count":1566},
                {"Name":"Lobster Salad","Count":1511},
                {"Name":"Chocolate","Count":1489},
                {"Name":"Apple Pie","Count":1487},
                {"Name":"Orange Juice","Count":1423},
                {"Name":"American Cheese","Count":1372},
                {"Name":"Green Peas","Count":1341},
                {"Name":"Assorted Cakes","Count":1331},
                {"Name":"French Fried Potatoes","Count":1328},
                {"Name":"Potato Salad","Count":1306},
                {"Name":"Baked Potatoes","Count":1293},
                {"Name":"Roquefort","Count":1273},
                {"Name":"Stewed Prunes","Count":1268}]
        };

        var diameter = 600;
        var color = d3.scaleOrdinal(d3.schemeCategory20);

        var bubble = d3.pack(dataset)
            .size([diameter, diameter])
            .padding(1.5);

        var svg = d3.select("#d3matrix").append("svg")
            .append("svg")
            .attr("width", diameter)
            .attr("height", diameter)
            .attr("class", "bubble");

        var nodes = d3.hierarchy(dataset)
            .sum(function(d) { return d.Count; });

        var node = svg.selectAll(".node")
            .data(bubble(nodes).descendants())
            .enter()
            .filter(function(d){
                return  !d.children
            })
            .append("g")
            .attr("class", "node")
            .attr("transform", function(d) {
                return "translate(" + d.x + "," + d.y + ")";
            });

        node.append("title")
            .text(function(d) {
                return d.Name + ": " + d.Count;
            });

        node.append("circle")
            .attr("r", function(d) {
                return d.r;
            })
            .style("fill", function(d,i) {
                return color(i);
            });

        node.append("text")
            .attr("dy", ".2em")
            .style("text-anchor", "middle")
            .text(function(d) {
                return d.data.Name.substring(0, d.r / 3);
            })
            .attr("font-family", "sans-serif")
            .attr("font-size", function(d){
                return d.r/5;
            })
            .attr("fill", "white");

        node.append("text")
            .attr("dy", "1.3em")
            .style("text-anchor", "middle")
            .text(function(d) {
                return d.data.Count;
            })
            .attr("font-family",  "Gill Sans", "Gill Sans MT")
            .attr("font-size", function(d){
                return d.r/5;
            })
            .attr("fill", "white");

        d3.select(self.frameElement)
            .style("height", diameter + "px");









        $scope.bubbleChart = "active";
        $scope.graph = null;
        $scope.viewResult = "active";
        loadDialog.close();
    };

    $scope.load();
});
app.factory('demoService', function ($http) {
    return {};
});

function classes(root) {
    var classes = [];

    function recurse(name, node) {
        if (node.children) node.children.forEach(function (child) {
            recurse(node.name, child);
        });
        else classes.push({packageName: name, name: node.name, count: node.count});
    }

    recurse(null, root);
    return {children: classes};
}









/*
Code extra

// var thresholdTime = 60000;
    // $scope.fileCreate = null;
    // $scope.errorInput = null;
    // $scope.sendInfo = function () {
    //     var loadDialog = document.getElementById("loading");
    //     loadDialog.showModal();
    //
    //     var hashtagText = document.getElementById('hashtagText').value;
    //     var timeRun1 = document.getElementById('timeRun1').value;
    //     var timeRun2 = document.getElementById('timeRun2').value;
    //     demoService.inputDati(hashtagText, timeRun1, timeRun2);
    //     $scope.fileCreate = "active";
    //     $scope.activeBubbleChart();
    //     loadDialog.close();
    // };

    // $scope.valuedInputDemo = function () {
    //     var hashtagText = document.getElementById('hashtagText').value;
    //     var timeRun1 = document.getElementById('timeRun1').value;
    //     var timeRun2 = document.getElementById('timeRun2').value;
    //
    //     if (hashtagText.charAt(0) != "#") {
    //         $scope.errorInput = "Il testo deve iniziare con #"
    //     } else if (hashtagText.length < 2) {
    //         $scope.errorInput = "Il testo deve contenere almeno un carattere dopo #"
    //     } else {
    //         $scope.errorInput = null;
    //     }
    //     var cond = (hashtagText != "" && timeRun1 >= thresholdTime && timeRun2 >= thresholdTime && $scope.errorInput == null);
    //     document.getElementById("submitInfoDemo").disabled = !cond;
    // };


app.factory('demoService', function ($http) {
    return {

        inputDati: function (hashtag, time1, time2) {
            var status = false;
            // var mysql = required('mysql');
            // var connection = mysql.createConnection({
            //     host: 'localhost',
            //     user: 'user',
            //     password: 'root',
            //     database: 'TweetAnalysisDB'
            // });
            //     connection.query('SELECT * FROM Input', (err, rows) = {
            //         if(err) {
            //             throw err;
            //         }
            //
            //         console.log('Data received from Db:\n');
            //     console.log(rows);
            // })
            //     ;
            return status;
        },
        queryBubbleChart: function () {
            return "";
        },
        queryGraph: function () {
            return "";
        }
    };
});

 */