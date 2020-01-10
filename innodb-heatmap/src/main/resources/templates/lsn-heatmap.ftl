<head>
    <!-- Plotly.js -->
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>

<body>

<div id="myDiv"><!-- Plotly chart will be drawn inside this DIV,
https://plot.ly/javascript/heatmaps/#basic-heatmap --></div>
<script>
var data = [
  {
    z: [
    <#if lsnlist?exists >
        <#list lsnlist as line>
            [
                <#list line.list as n>
                    ${n}
                    <#if n?index != line.list?size - 1>
                      ,
                    </#if>
                </#list>
            ]
            <#if line?index != lsnlist?size - 1>
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
    type: 'heatmap'
  }
];

Plotly.newPlot('myDiv', data, {width: ${width}, height: ${height}}, {});


</script>
</body>