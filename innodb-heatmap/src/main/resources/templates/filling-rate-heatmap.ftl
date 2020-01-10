<head>
    <!-- Plotly.js -->
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>

<body>

<div id="myDiv"><!-- Plotly chart will be drawn inside this DIV,
https://plot.ly/javascript/heatmaps/#basic-heatmap --></div>
<script>
var colorscaleValue = [
  [0, '#eeeeee'],
  [1, '#333333']
];

var data = [
  {
    z: [
    <#if fillingRateList?exists >
        <#list fillingRateList as line>
            [
                <#list line.list as n>
                    ${n}
                    <#if n?index != line.list?size - 1>
                      ,
                    </#if>
                </#list>
            ]
            <#if line?index != fillingRateList?size - 1>
              ,
            </#if>
        </#list>
    </#if>
    ],
    y: [
      <#list ylist as y>
          '${y}'
          <#if y?index != ylist?size - 1>
            ,
          </#if>
      </#list>
    ],
    type: 'heatmap',
    colorscale: colorscaleValue,
    showscale: true
  }
];

Plotly.newPlot('myDiv', data, {width: ${width}, height: ${height}}, {});


</script>
</body>