// Copyright 2011 Google Inc. All Rights Reserved.

/**
 * @fileoverview A JavaScript helper file that performs miscellaneous
 * functions - right now, it just keeps the form that runs MR jobs in sync with
 * the user's selection outside of the form.
 */

/*
 * Updates the form that runs MapReduce jobs once the user selects their input
 * data from the list of input files. Exists because we have two separate forms
 * on our HTML - one that allows users to upload new input files, and one that
 * allows users to run MapReduce jobs given a certain input file. Since the
 * latter form cannot see which input file has been selected (that button is
 * out of this form's scope), we throw some quick JavaScript in to sync the
 * value of the user's choice with a hidden field in the form as well as a
 * visible label displaying the input file's name for the user to see.
 * @param {string} filekey The internal key that the Datastore uses to reference
 *     this input file.
 * @param {string} blobkey The Blobstore key associated with the input file
 *     whose key is filekey.
 * @param {string} filename The name that the user has chosen to give this input
 *     file upon uploading it.
 */
function updateForm(filekey, blobkey, filename) {
  $('#jobName').text(filename);
  $('#filekey').val(filekey);
  $('#blobkey').val(blobkey);

  $('#word_count').removeAttr('disabled');
  $('#index').removeAttr('disabled');
  $('#phrases').removeAttr('disabled');
}

