/*
 * Copyright 2012 Google Inc.
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


function initRootList() {
  setButter('Loading...');
  $.ajax({
    type: 'GET',
    url: 'rpc/list' + window.location.search,
    dataType: 'text',
    error: function(request, textStatus) {
      getResponseDataJson(textStatus);
    },
    success: function(data, textStatus, request) {
      var response = getResponseDataJson(null, data);
      if (response) {
        clearButter();
        initRootListDone(response);
      }
    }
  });
}


function initRootListDone(response) {
  if (response.pipelines && response.pipelines.length > 0) {
    $('#root-list').show();
    if (response.cursor) {
      $('#next-link').attr('href', '?cursor=' + response.cursor).show();
    }

    $.each(response.pipelines, function(index, infoMap) {
      var row = $('<tr>');
      $('<td class="class-path">').text(infoMap.classPath).appendTo(row);
      $('<td class="status">').text(infoMap.status).appendTo(row);

      var sinceSpan = $('<abbr class="timeago">');
      var isoDate = getIso8601String(infoMap.startTimeMs);
      sinceSpan.attr('title', isoDate);
      sinceSpan.text(isoDate);
      sinceSpan.timeago();
      $('<td class="start-time">').append(sinceSpan).appendTo(row);

      $('<td class="run-time">').text(getElapsedTimeString(
          infoMap.startTimeMs, infoMap.endTimeMs)).appendTo(row);
      $('<td class="links">')
          .append(
            $('<a>')
                .attr('href', 'status?root=' + infoMap.pipelineId)
                .text('Detail'))
          .appendTo(row);
      $('#root-list>tbody').append(row);
    });
  } else {
    $('#empty-list-message').text('No pipelines found.').show();
  }
}
