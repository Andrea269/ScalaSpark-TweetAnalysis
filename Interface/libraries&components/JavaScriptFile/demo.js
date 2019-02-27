app = angular.module('demoApp', []);


app.controller('demoController', function ($scope, demoService) {


    var thresholdTime = 60000;
    $scope.fileCreate = null;
    $scope.errorInput = null;
    $scope.graph = null;
    $scope.bubbleChart = null;
    $scope.sendInfo = function () {
        var loadDialog = document.getElementById("loading");
        loadDialog.showModal();

        var hashtagText = document.getElementById('hashtagText').value;
        var timeRun1 = document.getElementById('timeRun1').value;
        var timeRun2 = document.getElementById('timeRun2').value;
        demoService.inputDati(hashtagText, timeRun1, timeRun2);
        $scope.fileCreate = "active";
        $scope.activeBubbleChart();
        loadDialog.close();
    };

    $scope.valuedInputDemo = function () {
        var hashtagText = document.getElementById('hashtagText').value;
        var timeRun1 = document.getElementById('timeRun1').value;
        var timeRun2 = document.getElementById('timeRun2').value;

        if (hashtagText.charAt(0) != "#") {
            $scope.errorInput = "Il testo deve iniziare con #"
        } else if (hashtagText.length < 2) {
            $scope.errorInput = "Il testo deve contenere almeno un carattere dopo #"
        } else {
            $scope.errorInput = null;
        }
        var cond = (hashtagText != "" && timeRun1 >= thresholdTime && timeRun2 >= thresholdTime && $scope.errorInput == null);
        document.getElementById("submitInfoDemo").disabled = !cond;
    };


    $scope.activeGraph = function () {

    };

    $scope.activeBubbleChart = function () {

    };
    $scope.resetView = function () {
        // $scope.fileCreate = null;
        $scope.errorInput = null;
        $scope.bubbleChart = null;
        $scope.graph = null;
    };
});
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
        }
    };
});