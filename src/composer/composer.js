function TGraph( name, size, start, end, res) {
    //properties
    this._name = name || "";
    this._size = size || 1;
    this._start = start || 2016;
    this._end = end || 2016;
    this._res = res || 1;
    this._changed = false;

    this.getName = function() { return this._name; }
    this.setName = function( name ) { this._name = name; }
    this.getSize = function() { return this._size; }
    this.getStart = function() { return this._start; }
    this.getEnd = function() { return this._end; }
    this.getRes = function() { return this._res; }
    this.getChanged = function() { return this._changed; }
    this.setChanged = function() { this._changed = true; }
}

var graphMap = {};
initializeMap();

function initializeMap() {
    var dblpTG = new TGraph("dblp", 80, 1936, 2015, 1);
    saveGraph(dblpTG);
    
    var arxiv = new TGraph("arXiv", 23, 1993, 2016, 1);
    saveGraph(arxiv);
}

function saveGraph( graph ) {
    graphMap[graph.getName()] = graph;
}

function lookupGraph( name ) {
    return graphMap[name];
}

function makeTimeline( tg ) {
    var elems;

    if (tg.size == 1)  {
	elems = '<td class="start end">' + tg.getStart() + ' -</td>';
    } else if (tg.size < 6) {
	elems = '<td class="start">' + tg.start + ' -</td>';
	for (i=tg.getStart() + tg.getRes(); i < tg.getEnd(); i+tg.getRes()) {
	    elems = elems + '<td>' + i + '</td>';
	}
	elems = elems + '<td class="end">' + tg.getEnd() + ' -</td>';
    } else {
	elems = '<td class="start">' + tg.getStart() + ' -</td><td>' + (tg.getStart() + tg.getRes()) + ' -</td><td class="skip">...</td><td>' + (tg.getEnd() - tg.getRes()) + ' -</td><td class="end">' + tg.getEnd() + ' -</td>';
    }

    var table = $('<table id=' + tg.getName() + '><tr><td class="tlinetitle">' + tg.getName() + '&nbsp;</td><td class="timeborder" id=' + tg.getName() + 'start><hr width=1 size=35 style="padding:none; margin:none" color=grey></td>' + elems + '<td class="timeborder" id=' + tg.getName() + 'end><hr width=1 size=35 color=grey></td></tr></table>');
    table.draggable({ containment: "#workspace" });
    $( "#workspace" ).append(table);

    $( "#" + tg.getName() + "start").draggable({ 
	containment: "parent",
	snap: true,
	axis: "x",
	stop: function( event, ui ) {
	    //get all the td elements that are left of the position, make them not selected
	    var tg = lookupGraph( event.target.id.substring(0, event.target.id.indexOf("start")));
	    //mark the tg as edited/changed
	    tg.setChanged();
	    //need to update the view in the table
	    //FIXME: pick id that's not used
	    $(table).children().children().children(".tlinetitle").html( tg.getName() + "-1*&nbsp;" );
	    //now make all the tds to the left unselected
	    $(table).children().children().children().each( function() {
		if (Math.abs($(this).position().left - ui.position.left) < 20) {
		    $(event.target).position({"my" : "left", "at" : "right", "of" : $(this)});
		}
		//TODO: make this smoother
		if ($(this).position().left < ui.position.left) {
		    $(this).addClass("notselected");
		}
	    });
	}
    });
    $( "#" + tg.getName() + "end").draggable({ 
	containment: "parent", 
	snap: true,
	axis: "x",
	stop: function( event, ui ) {
	    //get all the td elements that are right of the position, make them not selected
	    var tg = lookupGraph( event.target.id.substring(0, event.target.id.indexOf("end")));
	    //mark the tg as edited/changed
	    tg.setChanged();
	    //need to update the view in the table
	    //FIXME: pick id that's not used
	    $(table).children().children().children(".tlinetitle").html( tg.getName() + "-1*&nbsp;" );
	    //now make all the tds to the right unselected
	    var dragPos = $(this).position().left
	    $(table).children().children().children().each( function() {
		if (Math.abs($(this).position().left - dragPos) < 20) {
		    $(event.target).position({"my" : "right", "at" : "left", "of" : $(this)});
		}
		//TODO: make this smoother
		if ($(this).position().left > dragPos) {
		    $(this).addClass("notselected");
		}
	    });
	}
    });
}


$( "#tabs" ).tabs();
$( "#views-list" ).menu({
  select: function( event, ui ) {
      makeTimeline(lookupGraph(ui.item.text()));
      //TODO: add the schema for this graph onto the schema area
  }
});
$( "div.attributes" ).buttonset();

$( "[id$='start']").draggable({
  containment: "parent"
});
$( "[id$='end']").draggable({
  containment: "parent"
});
$( "div.join" ).draggable({
 containment: "parent"
});
$( "table" ).draggable({
  containment: "#workspace"
});

$( "#accordion" ).accordion({
  heightStyle: 'panel'
});

$( "#T3namemenu" ).selectmenu();



