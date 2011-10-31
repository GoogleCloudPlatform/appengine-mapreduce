/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 */

// Global variables.
var AUTO_REFRESH = true;
var ROOT_PIPELINE_ID = null;
var STATUS_MAP = null;


// Adjusts the height/width of the embedded status console iframe.
function adjustStatusConsole() {
  var statusConsole = $('#status-console');
  var detail = $('#detail');
  var sidebar = $('#sidebar');
  var control = $('#control');

  // NOTE: 16 px here is the height of the resize grip in most browsers.
  // Need to specify this explicitly because some browsers (eg, Firefox)
  // cause the overflow scrollbars to bounce around the page randomly when
  // they accidentally overlap the resize grip.
  if (statusConsole.css('display') == 'none') {
    var paddingAndMargin = detail.outerHeight() - detail.height() + 16;
    detail.css('max-height', (sidebar.outerHeight() - paddingAndMargin) + 'px');
  } else {
    detail.css('max-height', '200px');
    statusConsole.width(
        $(window).width() - sidebar.outerWidth());
    statusConsole.height(
        $(window).height() - (statusConsole.offset().top + 16));
  }
}


// Gets the ID of the pipeline info in the left-nav.
function getTreePipelineElementId(value) {
  if (value.indexOf('#item-pipeline-') == 0) {
    return value;
  } else {
    return '#item-pipeline-' + value;
  }
}


// Scrolls to element of the pipeline in the tree.
function scrollTreeToPipeline(pipelineIdOrElement) {
  var element = pipelineIdOrElement;
  if (!(pipelineIdOrElement instanceof jQuery)) {
    element = $(getTreePipelineElementId(pipelineIdOrElement));
  }
  $('#sidebar').scrollTop(element.attr('offsetTop'));
  $('#sidebar').scrollLeft(element.attr('offsetLeft'));
}


// Opens all pipelines down to the target one if not already expanded and
// scroll that pipeline into view.
function expandTreeToPipeline(pipelineId) {
  if (pipelineId == null) {
    return;
  }
  var elementId = getTreePipelineElementId(pipelineId);
  var parents = $(elementId).parents('.expandable');
  if (parents.size() > 0) {
    // The toggle function will scroll to highlight the pipeline.
    parents.children('.hitarea').click();
  } else {
    // No children, so just scroll.
    scrollTreeToPipeline(pipelineId);
  }
}


// Handles when the user toggles a leaf of the tree.
function handleTreeToggle(index, element) {
  var parentItem = $(element).parent();
  var collapsing = parentItem.hasClass('expandable');
  if (collapsing) {
  } else {
    // When expanded be sure the pipeline and its children are showing.
    scrollTreeToPipeline(parentItem);
  }
}


// Constructs the info div for a stage.
function constructStageNode(pipelineId, infoMap, sidebar) {
  var containerDiv = $('<div class="status-box">');
  containerDiv.addClass('status-' + infoMap.status);

  var detailDiv = $('<div class="detail-link">');
  var detailLink = $('<a>');
  detailLink.attr('href', '#pipeline-' + pipelineId);
  detailLink.attr('title', 'ID #' + pipelineId);
  detailLink.attr('id', 'link-pipeline-' + pipelineId);
  var nameWithBreaks = infoMap.classPath.replace(/\./, '.<wbr>');
  detailLink.html(nameWithBreaks);
  detailDiv.append(detailLink);
  containerDiv.append(detailDiv);

  // Broad status category.
  var statusTitleDiv = $('<div class="status-title">');
  if (!sidebar) {
    statusTitleDiv.append($('<span>').text('Status: '));
  }
  statusTitleDiv.append($('<span>').text(infoMap.status));
  containerDiv.append(statusTitleDiv);

  // ID of the pipeline
  if (!sidebar) {
    var pipelineIdDiv = $('<div class="status-pipeline-id">');
    pipelineIdDiv.text('ID #' + pipelineId);
    containerDiv.append(pipelineIdDiv);
  }

  // Determine timing information based on state.
  var statusMessage = null;
  var statusTimeLabel = null;
  var statusTimeMs = null;
  var statusRuntimeDiv = null

  if (infoMap.status == 'done') {
    statusRuntimeDiv = $('<div class="status-runtime">');

    var statusTimeSpan = $('<span class="status-time-label">');
    statusTimeSpan.text('Run time: ');
    statusRuntimeDiv.append(statusTimeSpan);

    var runtimeSpan = $('<span class="status-runtime-value">');
    runtimeSpan.text(getElapsedTimeString(
        infoMap.startTimeMs, infoMap.endTimeMs));
    statusRuntimeDiv.append(runtimeSpan);

    statusTimeLabel = 'Complete';
    statusTimeMs = infoMap.endTimeMs;
    if (infoMap.statusMessage) {
      statusMessage = infoMap.statusMessage;
    }
  } else if (infoMap.status == 'run') {
    statusTimeLabel = 'Started';
    statusTimeMs = infoMap.startTimeMs;
    if (infoMap.statusMessage) {
      statusMessage = infoMap.statusMessage;
    }
  } else if (infoMap.status == 'retry') {
    statusTimeLabel = 'Will run';
    statusTimeMs = infoMap.startTimeMs;
    if (infoMap.lastRetryMessage) {
      statusMessage = infoMap.lastRetryMessage;
    }
  } else if (infoMap.status == 'finalizing') {
    statusTimeLabel = 'Complete';
    statusTimeMs = infoMap.endTimeMs;
    if (infoMap.statusMessage) {
      statusMessage = infoMap.statusMessage;
    }
  } else if (infoMap.status == 'aborted') {
    statusTimeLabel = 'Aborted';
    statusTimeMs = infoMap.endTimeMs;
    if (infoMap.abortMessage) {
      statusMessage = infoMap.abortMessage;
    }
  } else if (infoMap.status == 'waiting') {
    // Do nothing.
  }

  // User-supplied status message.
  if (statusMessage) {
    var statusMessageDiv = $('<div class="status-message">');
    statusMessageDiv.text(statusMessage);
    containerDiv.append(statusMessageDiv);
  }

  // Runtime if present.
  if (statusRuntimeDiv) {
    containerDiv.append(statusRuntimeDiv);
  }

  // Next retry time, complete time, start time.
  if (statusTimeLabel && statusTimeMs) {
    var statusTimeDiv = $('<div class="status-time">');

    var statusTimeSpan = $('<span class="status-time-label">');
    statusTimeSpan.text(statusTimeLabel + ': ');
    statusTimeDiv.append(statusTimeSpan);

    var sinceSpan = $('<abbr class="timeago status-time-since">');
    var isoDate = getIso8601String(statusTimeMs);
    sinceSpan.attr('title', isoDate);
    sinceSpan.text(isoDate);
    sinceSpan.timeago();
    statusTimeDiv.append(sinceSpan);

    containerDiv.append(statusTimeDiv);
  }

  // User-supplied status links.
  var linksDiv = $('<div class="status-links">');
  if (!sidebar) {
    linksDiv.append($('<span>').text('Links: '));
  }
  var foundLinks = 0;
  if (infoMap.statusConsoleUrl) {
    var link = $('<a class="status-console">');
    link.attr('href', infoMap.statusConsoleUrl);
    link.text('Console');
    link.click(function(event) {
      selectPipeline(pipelineId);
      event.preventDefault();
    });
    linksDiv.append(link);
    foundLinks++;
  }
  if (infoMap.statusLinks) {
    $.each(infoMap.statusLinks, function(key, value) {
      var link = $('<a>');
      link.attr('href', value);
      link.text(key);
      link.click(function(event) {
        selectPipeline(pipelineId, key);
        event.preventDefault();
      });
      linksDiv.append(link);
      foundLinks++;
    });
  }
  if (foundLinks > 0) {
    containerDiv.append(linksDiv);
  }

  // Retry parameters.
  if (!sidebar) {
    var retryParamsDiv = $('<div class="status-retry-params">');
    retryParamsDiv.append(
        $('<div class="retry-params-title">').text('Retry parameters'));

    var backoffSecondsDiv = $('<div class="retry-param">');
    $('<span>').text('Backoff seconds: ').appendTo(backoffSecondsDiv);
    $('<span>')
        .text(infoMap.backoffSeconds)
        .appendTo(backoffSecondsDiv);
    retryParamsDiv.append(backoffSecondsDiv);

    var backoffFactorDiv = $('<div class="retry-param">');
    $('<span>').text('Backoff factor: ').appendTo(backoffFactorDiv);
    $('<span>')
        .text(infoMap.backoffFactor)
        .appendTo(backoffFactorDiv);
    retryParamsDiv.append(backoffFactorDiv);

    containerDiv.append(retryParamsDiv);
  }

  function renderCollapsableValue(value, container) {
    var stringValue = $.toJSON(value);
    var SPLIT_LENGTH = 200;
    if (stringValue.length < SPLIT_LENGTH) {
      container.append($('<span>').text(stringValue));
      return;
    }

    var startValue = stringValue.substr(0, SPLIT_LENGTH);
    var endValue = stringValue.substr(SPLIT_LENGTH);

    // Split the end value with <wbr> tags so it looks nice; force
    // word wrapping never works right.
    var moreSpan = $('<span class="value-disclosure-more">');
    for (var i = 0; i < endValue.length; i += SPLIT_LENGTH) {
      moreSpan.append(endValue.substr(i, SPLIT_LENGTH));
      moreSpan.append('<wbr/>');
    }
    var betweenMoreText = '...(' + endValue.length + ' more) ';
    var betweenSpan = $('<span class="value-disclosure-between">')
        .text(betweenMoreText);
    var toggle = $('<a class="value-disclosure-toggle">')
        .text('Expand')
        .attr('href', '');
    toggle.click(function(e) {
        e.preventDefault();
        if (moreSpan.css('display') == 'none') {
          betweenSpan.text(' ');
          toggle.text('Collapse');
        } else {
          betweenSpan.text(betweenMoreText);
          toggle.text('Expand');
        }
        moreSpan.toggle();
    });
    container.append($('<span>').text(startValue));
    container.append(moreSpan);
    container.append(betweenSpan);
    container.append(toggle);
  }

  // Slot rendering
  function renderSlot(slotKey) {
    var filledMessage = null;
    var slot = STATUS_MAP.slots[slotKey];
    var slotDetailDiv = $('<div class="slot-detail">');
    if (!slot) {
      var keyAbbr = $('<abbr>');
      keyAbbr.attr('title', slotKey);
      keyAbbr.text('Pending slot');
      slotDetailDiv.append(keyAbbr);
      return slotDetailDiv;
    }

    if (slot.status == 'filled') {
      var valueDiv = $('<span class="slot-value-container">');
      valueDiv.append($('<span>').text('Value: '));
      var valueContainer = $('<span class="slot-value">');
      renderCollapsableValue(slot.value, valueContainer);
      valueDiv.append(valueContainer);
      slotDetailDiv.append(valueDiv);

      var filledDiv = $('<div class="slot-filled">');
      filledDiv.append($('<span>').text('Filled: '));
      var isoDate = getIso8601String(slot.fillTimeMs);
      filledDiv.append(
          $('<abbr class="timeago">')
              .attr('title', isoDate)
              .text(isoDate)
              .timeago());
      slotDetailDiv.append(filledDiv);

      filledMessage = 'Filled by';
    } else {
      filledMessage = 'Waiting for';
    }

    var filledMessageDiv = $('<div class="slot-message">');
    filledMessageDiv.append(
        $('<span>').text(filledMessage + ': '));
    var otherPipeline = STATUS_MAP.pipelines[slot.fillerPipelineId];
    if (otherPipeline) {
      var fillerLink = $('<a class="slot-filler">');
      fillerLink
          .attr('title', 'ID #' + slot.fillerPipelineId)
          .attr('href', '#pipeline-' + slot.fillerPipelineId)
          .text(otherPipeline.classPath);
      fillerLink.click(function(event) {
          selectPipeline(slot.fillerPipelineId);
          event.preventDefault();
      });
      filledMessageDiv.append(fillerLink);
    } else {
      filledMessageDiv.append(
          $('<span class="status-pipeline-id">')
              .text('ID #' + slot.fillerPipelineId));
    }
    slotDetailDiv.append(filledMessageDiv);
    return slotDetailDiv;
  }

  // Argument/ouptut rendering
  function renderParam(key, valueDict) {
    var paramDiv = $('<div class="status-param">');

    var nameDiv = $('<span class="status-param-name">');
    nameDiv.text(key + ':');
    paramDiv.append(nameDiv);

    if (valueDict.type == 'slot' && STATUS_MAP.slots) {
      paramDiv.append(renderSlot(valueDict.slotKey));
    } else {
      var valueDiv = $('<span class="status-param-value">');
      renderCollapsableValue(valueDict.value, valueDiv);
      paramDiv.append(valueDiv);
    }

    return paramDiv;
  }

  if (!sidebar && (
      !$.isEmptyObject(infoMap.kwargs) || infoMap.args.length > 0)) {
    var paramDiv = $('<div class="param-container">');
    paramDiv.append(
        $('<div class="param-container-title">')
            .text('Parameters'));

    // Positional arguments
    $.each(infoMap.args, function(index, valueDict) {
      paramDiv.append(renderParam(index, valueDict));
    });

    // Keyword arguments in alphabetical order
    var keywordNames = [];
    $.each(infoMap.kwargs, function(key, value) {
      keywordNames.push(key);
    });
    keywordNames.sort();
    $.each(keywordNames, function(index, key) {
      paramDiv.append(renderParam(key, infoMap.kwargs[key]));
    });

    containerDiv.append(paramDiv);
  }

  // Outputs in alphabetical order, but default first
  if (!sidebar) {
    var outputContinerDiv = $('<div class="outputs-container">');
    outputContinerDiv.append(
        $('<div class="outputs-container-title">')
            .text('Outputs'));

    var outputNames = [];
    $.each(infoMap.outputs, function(key, value) {
      if (key != 'default') {
        outputNames.push(key);
      }
    });
    outputNames.sort();
    outputNames.unshift('default');

    $.each(outputNames, function(index, key) {
      outputContinerDiv.append(renderParam(
            key, {'type': 'slot', 'slotKey': infoMap.outputs[key]}));
    });

    containerDiv.append(outputContinerDiv);
  }

  // Related pipelines
  function renderRelated(relatedList, relatedTitle, classPrefix) {
    var relatedDiv = $('<div>');
    relatedDiv.addClass(classPrefix + '-container');
    relatedTitleDiv = $('<div>');
    relatedTitleDiv.addClass(classPrefix + '-container-title');
    relatedTitleDiv.text(relatedTitle);
    relatedDiv.append(relatedTitleDiv);

    $.each(relatedList, function(index, relatedPipelineId) {
      var relatedInfoMap = STATUS_MAP.pipelines[relatedPipelineId];
      if (relatedInfoMap) {
        var relatedLink = $('<a>');
        relatedLink
            .addClass(classPrefix + '-link')
            .attr('title', 'ID #' + relatedPipelineId)
            .attr('href', '#pipeline-' + relatedPipelineId)
            .text(relatedInfoMap.classPath);
        relatedLink.click(function(event) {
            selectPipeline(relatedPipelineId);
            event.preventDefault();
        });
        relatedDiv.append(relatedLink);
      } else {
        var relatedIdDiv = $('<div>');
        relatedIdDiv
            .addClass(classPrefix + '-pipeline-id')
            .text('ID #' + relatedPipelineId);
        relatedDiv.append(relatedIdDiv);
      }
    });

    return relatedDiv;
  }

  // Run after
  if (!sidebar && infoMap.afterSlotKeys.length > 0) {
    var foundPipelineIds = [];
    $.each(infoMap.afterSlotKeys, function(index, slotKey) {
      if (STATUS_MAP.slots[slotKey]) {
        var slotDict = STATUS_MAP.slots[slotKey];
        if (slotDict.fillerPipelineId) {
          foundPipelineIds.push(slotDict.fillerPipelineId);
        }
      }
    });
    containerDiv.append(
        renderRelated(foundPipelineIds, 'Run after', 'run-after'));
  }

  // Spawned children
  if (!sidebar && infoMap.children.length > 0) {
    containerDiv.append(
        renderRelated(infoMap.children, 'Children', 'child'));
  }

  return containerDiv;
}


// Recursively creates the sidebar. Use null nextPipelineId to create from root.
function generateSidebar(statusMap, nextPipelineId, rootElement) {
  var currentElement = null;

  if (nextPipelineId) {
    currentElement = $('<li>');
    // Value should match return of getTreePipelineElementId
    currentElement.attr('id', 'item-pipeline-' + nextPipelineId);
  } else {
    currentElement = rootElement;
    nextPipelineId = statusMap.rootPipelineId;
  }

  var parentInfoMap = statusMap.pipelines[nextPipelineId];
  currentElement.append(
      constructStageNode(nextPipelineId, parentInfoMap, true));

  var children = statusMap.pipelines[nextPipelineId].children;
  if (children.length > 0) {
    var treeElement = null;
    if (rootElement) {
      treeElement =
          $('<ul id="pipeline-tree" class="treeview-black treeview">');
    } else {
      treeElement = $('<ul>');
    }

    $.each(children, function(index, childPipelineId) {
      var childElement = generateSidebar(statusMap, childPipelineId);
      treeElement.append(childElement);
    });
    currentElement.append(treeElement);
  }

  return currentElement;
}



function selectPipeline(pipelineId, linkName) {
  if (linkName) {
    location.hash = '#pipeline-' + pipelineId + ';' + linkName;
  } else {
    location.hash = '#pipeline-' + pipelineId;
  }
}


// Depth-first search for active pipeline.
function findActivePipeline(pipelineId, isRoot) {
  var infoMap = STATUS_MAP.pipelines[pipelineId];
  if (!infoMap) {
    return null;
  }

  // This is an active leaf node.
  if (infoMap.children.length == 0 && infoMap.status != 'done') {
    return pipelineId;
  }

  // Sort children by start time only.
  var children = infoMap.children.slice(0);
  children.sort(function(a, b) {
    var infoMapA = STATUS_MAP.pipelines[a];
    var infoMapB = STATUS_MAP.pipelines[b];
    if (!infoMapA || !infoMapB) {
      return 0;
    }
    if (infoMapA.startTimeMs && infoMapB.startTimeMs) {
      return infoMapA.startTimeMs - infoMapB.startTimeMs;
    } else {
      return 0;
    }
  })

  for (var i = 0; i < children.length; ++i) {
    var foundPipelineId = findActivePipeline(children[i], false);
    if (foundPipelineId != null) {
      return foundPipelineId;
    }
  }

  return null;
}


function getSelectedPipelineId() {
  var prefix = '#pipeline-';
  var pieces = location.hash.split(';', 2);
  if (pieces[0].indexOf(prefix) == 0) {
    return pieces[0].substr(prefix.length);
  }
  return null;
}


/* Event handlers */
function handleHashChange() {
  var prefix = '#pipeline-';
  var hash = location.hash;
  var pieces = hash.split(';', 2);
  var pipelineId = null;

  if (pieces[0].indexOf(prefix) == 0) {
    pipelineId = pieces[0].substr(prefix.length);
  } else {
    // Bad hash, just show the root pipeline.
    location.hash = '';
    return;
  }

  if (!pipelineId) {
    // No hash means show the root pipeline.
    pipelineId = STATUS_MAP.rootPipelineId;
  }
  var rootMap = STATUS_MAP.pipelines[STATUS_MAP.rootPipelineId];
  var infoMap = STATUS_MAP.pipelines[pipelineId];
  if (!rootMap || !infoMap) {
    // Hash not found.
    return;
  }

  // Clear any selection styling.
  $('.selected-link').removeClass('selected-link');

  if (pieces[1]) {
    // Show a specific status link.
    var statusLink = $(getTreePipelineElementId(pipelineId))
        .find('.status-links>a:contains("' + pieces[1] + '")');
    if (statusLink.size() > 0) {
      var selectedLink = $(statusLink[0]);
      selectedLink.addClass('selected-link');
      $('#status-console').attr('src', selectedLink.attr('href'));
      $('#status-console').show();
    } else {
      // No console link for this pipeline; ignore it.
      $('#status-console').hide();
    }
  } else {
    // Show the console link.
    var consoleLink = $(getTreePipelineElementId(pipelineId))
        .find('a.status-console');
    if (consoleLink.size() > 0) {
      var selectedLink = $(consoleLink[0]);
      selectedLink.addClass('selected-link');
      $('#status-console').attr('src', selectedLink.attr('href'));
      $('#status-console').show();
    } else {
      // No console link for this pipeline; ignore it.
      $('#status-console').hide();
    }
  }

  // Mark the pipeline as selected.
  $('#link-pipeline-' + pipelineId).addClass('selected-link');

  // Title is always the info for the root pipeline, to make it easier to
  // track across multiple tabs.
  document.title = rootMap.classPath + ' - ID #' + STATUS_MAP.rootPipelineId;

  // Update the detail status frame.
  var stageNode = constructStageNode(pipelineId, infoMap, false);
  $('#overview').remove();
  stageNode.attr('id', 'overview')
  $('#detail').append(stageNode);

  // Make sure everything is the right size.
  adjustStatusConsole();
}


function handleAutoRefreshClick(event) {
  var loc = window.location;
  var newSearch = null;
  if (!AUTO_REFRESH && event.target.checked) {
    newSearch = '?root=' + ROOT_PIPELINE_ID;
  } else if (AUTO_REFRESH && !event.target.checked) {
    newSearch = '?root=' + ROOT_PIPELINE_ID + '&auto=false';
  }

  if (newSearch != null) {
    loc.replace(
        loc.protocol + '//' + loc.host + loc.pathname +
        newSearch + loc.hash);
  }
}


function handleRefreshClick(event) {
  var loc = window.location;
  if (AUTO_REFRESH) {
    newSearch = '?root=' + ROOT_PIPELINE_ID;
  } else {
    newSearch = '?root=' + ROOT_PIPELINE_ID + '&auto=false';
  }
  loc.href = loc.protocol + '//' + loc.host + loc.pathname + newSearch;
  return false;
}


/* Initialization. */
function initStatus() {
  if (window.location.search.length > 0 &&
      window.location.search[0] == '?') {
    var query = window.location.search.substr(1);
    var pieces = query.split('&');
    $.each(pieces, function(index, param) {
      var mapping = param.split('=');
      if (mapping.length != 2) {
        return;
      }
      if (mapping[0] == 'auto' && mapping[1] == 'false') {
        AUTO_REFRESH = false;
      } else if (mapping[0] == 'root') {
        ROOT_PIPELINE_ID = mapping[1];
      }
    });
  }

  setButter('Loading... #' + ROOT_PIPELINE_ID);
  $.ajax({
    type: 'GET',
    url: 'rpc/tree?root_pipeline_id=' + ROOT_PIPELINE_ID,
    dataType: 'text',
    error: function(request, textStatus) {
      getResponseDataJson(textStatus);
    },
    success: function(data, textStatus, request) {
      var response = getResponseDataJson(null, data);
      if (response) {
        clearButter();
        STATUS_MAP = response;
        initStatusDone();
      }
    }
  });
}


function initStatusDone() {
  jQuery.timeago.settings.allowFuture = true;

  // Generate the sidebar.
  generateSidebar(STATUS_MAP, null, $('#sidebar'));

  // Turn the sidebar into a tree.
  $('#pipeline-tree').treeview({
    collapsed: true,
    unique: false,
    cookieId: 'pipeline Id here',
    toggle: handleTreeToggle,
  });
  $('#sidebar').show();

  // Init the control panel.
  $('#auto-refresh').click(handleAutoRefreshClick);
  if (!AUTO_REFRESH) {
    $('#auto-refresh').attr('checked', '');
  } else {
    var rootStatus = STATUS_MAP.pipelines[STATUS_MAP.rootPipelineId].status;
    if (rootStatus != 'done' && rootStatus != 'aborted') {
      // Only do auto-refresh behavior if we're not in a terminal state.
      window.setTimeout(function() {
        window.location.replace('');
      }, 30 * 1000);
    }
  }
  $('.refresh-link').click(handleRefreshClick);
  $('#control').show();

  // Properly adjust the console iframe to match the window size.
  $(window).resize(adjustStatusConsole);
  window.setTimeout(adjustStatusConsole, 0);

  // Handle ajax-y URL fragment events.
  $(window).hashchange(handleHashChange);
  $(window).hashchange();  // Trigger for initial load.

  // When there's no hash selected, auto-navigate to the most active node.
  if (window.location.hash == '') {
    var activePipelineId = findActivePipeline(STATUS_MAP.rootPipelineId, true);
    if (activePipelineId) {
      selectPipeline(activePipelineId);
    } else {
      // If there's nothing active, then select the root.
      selectPipeline(ROOT_PIPELINE_ID);
    }
  }

  // Scroll to the current active node.
  expandTreeToPipeline(getSelectedPipelineId());
}
