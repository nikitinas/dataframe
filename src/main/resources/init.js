var dataFrames = {  }

function addTable(df) {
    let cols = df.cols;
    for(let i=0;i<cols.length;i++){
        for(let c of cols[i].children){
            cols[c].parent = i;
        }
    }
    df.nrow = 0
    for(let i=0;i<df.cols.length;i++){
        if(df.cols[i].values.length > df.nrow) df.nrow = df.cols[i].values.length
    }
    dataFrames[df.id] = df
    render(df)
}

function computeRenderData(df) {
    let result = []
    let pos = 0
    for(let col=0;col<df.cols.length;col++){
        if(df.cols[col].parent == undefined)
            pos += computeRenderDataRec(df.cols, col, pos, 0, result, false, false)
    }
    return result
}

function computeRenderDataRec(cols, colId, pos, depth, result, leftBorder, rightBorder) {
    if(result.length == depth){
        var array = []
        if(pos > 0)
        {
            let j = 0
            for(let i = 0; j < pos; i++)
            {
                let c = result[depth-1][i]
                j += c.span
                let copy = Object.assign({empty: true}, c)
                array.push(copy)
            }
        }
        result.push(array)
    }
    var col = cols[colId]
    var size = 0
    if(col.expanded)
    {
        let childPos = pos
        for(let i=0;i<col.children.length;i++){
            let child = col.children[i]
            let childLeft = i == 0 && (col.children.length > 1 || leftBorder)
            let childRight = i == col.children.length-1 && (col.children.length > 1 || rightBorder)
            let childSize = computeRenderDataRec(cols, child, childPos, depth+1, result, childLeft, childRight)
            childPos += childSize
            size += childSize
        }
    }
    else{
        for(let i = depth+1;i<result.length;i++)
            result[i].push({id: colId, span: 1, leftBd: leftBorder, rightBd: rightBorder, empty:true})
        size = 1
    }
    let left = leftBorder
    let right = rightBorder
    if(size > 1) {
        left = true
        right = true
    }
    result[depth].push({id: colId, span: size, leftBd: left, rightBd: right})
    return size
}

function render(df) {

    // header
    var header = document.getElementById("df_header_" + df.id);
    header.innerHTML = ""
    let renderData = computeRenderData(df)
    for(let rowData of renderData){
        var tr = document.createElement("tr");
        header.appendChild(tr);
        for(let i = 0;i<rowData.length;i++) {
            cell = rowData[i]
            if(i < rowData.length-1){
                nextData = rowData[i+1]
                if(nextData.leftBd) cell.rightBd = true
                else if(cell.rightBd) cell.leftBd = true
            }
            var th = document.createElement("th")
            th.setAttribute("colspan", cell.span)
            let colId = cell.id
            let col = df.cols[colId];
            if(!cell.empty){
                var aClass = col.expanded ? " class='expanded'" : ""
                th.innerHTML = col.children.length == 0 ? col.name : "<a" + aClass + " onClick='expandCol(" + df.id + ", " + colId + ");'>" + col.name + "</a>"
            }
            let classes = (cell.leftBd ? " leftBorder" : "") + (cell.rightBd ? " rightBorder" : "")
            if(col.rightAlign)
                classes += " rightAlign"
            if(classes.length > 0)
                th.setAttribute("class", classes)
            tr.appendChild(th)
        }
    }

    // data
    var body = document.getElementById("df_body_" + df.id);
    body.innerHTML = ""
    var columns = renderData.pop()
    for(let row = 0; row < df.nrow; row++) {
        let tr = document.createElement("tr");
        body.appendChild(tr)
        for(let i = 0; i<columns.length;i++) {
            let cell = columns[i]
            let td = document.createElement("td");
            let colId = cell.id
            let col = df.cols[colId]
            let classes = (cell.leftBd ? " leftBorder" : "") + (cell.rightBd ? " rightBorder" : "")
            if(col.rightAlign)
                classes += " rightAlign"
            if(classes.length > 0)
                td.setAttribute("class", classes)
            td.innerHTML = df.cols[colId].values[row]
            tr.appendChild(td)
        }
    }
}

function expandCol(dfId, colId) {
    let df = dataFrames[dfId]
    df.cols[colId].expanded = !df.cols[colId].expanded
    render(df)
}