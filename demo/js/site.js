"use strict";

$(document).ready(function () {
	/* Video Lightbox */
	if (!!$.prototype.simpleLightboxVideo) {
		$('.video').simpleLightboxVideo();
	}

	/*ScrollUp*/
	if (!!$.prototype.scrollUp) {
		$.scrollUp();
	}

	/*Responsive Navigation*/
	$("#nav-mobile").html($("#nav-main").html());
	$("#nav-trigger span").on("click",function() {
		if ($("nav#nav-mobile ul").hasClass("expanded")) {
			$("nav#nav-mobile ul.expanded").removeClass("expanded").slideUp(250);
			$(this).removeClass("open");
		} else {
			$("nav#nav-mobile ul").addClass("expanded").slideDown(250);
			$(this).addClass("open");
		}
	});

	$("#nav-mobile").html($("#nav-main").html());
	$("#nav-mobile ul a").on("click",function() {
		if ($("nav#nav-mobile ul").hasClass("expanded")) {
			$("nav#nav-mobile ul.expanded").removeClass("expanded").slideUp(250);
			$("#nav-trigger span").removeClass("open");
		}
	});

	/* Sticky Navigation */
	if (!!$.prototype.stickyNavbar) {
		$('#header').stickyNavbar();
	}

	$('#content').waypoint(function (direction) {
		if (direction === 'down') {
			$('#header').addClass('nav-solid fadeInDown');
		}
		else {
			$('#header').removeClass('nav-solid fadeInDown');
		}
	});

});


/* Preloader and animations */
$(window).load(function () { // makes sure the whole site is loaded
	$('#status').fadeOut(); // will first fade out the loading animation
	$('#preloader').delay(350).fadeOut('slow'); // will fade out the white DIV that covers the website.
	$('body').delay(350).css({'overflow-y': 'visible'});

	/* WOW Elements */
	if (typeof WOW == 'function') {
		new WOW().init();
	}

	/* Parallax Effects */
	if (!!$.prototype.enllax) {
		$(window).enllax();
	}

});

$(window).load(function(){
	var uploadFileUrl= 'http://54.234.245.100:5000/xtH75NqswHKuu3fVKWJ4La'
	$.fn.dragAndDrop = function(p){
  var parameters = {
    'supported' : ['image/jpg', 'image/jpeg'],
    'size' : 5,
    'uploadFile' : uploadFileUrl,
    'sizeAlert' : 'File too heavy',
    'formatAlert' : 'Format not supported',
    'done' : function (msg) {
      console.info('upload done');
    },
    'error' : function (msg) {
      console.info('upload fail');
    },
    'onProgress' : function(progress){
      console.info(Math.round(progress * 100)+'%');
    }
  };
  let fd = ''
  $.extend(parameters,p);

  function upload(fd){
    $.ajax({
      type: 'POST',
      url: parameters.uploadFile,
      data: fd,
      processData: false,
      contentType: false,
      xhr: function()
      {
        var xhr = new window.XMLHttpRequest();
				//  xhr.responseType= 'blob'
        xhr.upload.addEventListener("progress", function(evt){
          if (evt.lengthComputable) {
            var percentComplete = evt.loaded / evt.total;
            parameters.onProgress(percentComplete);
          }
        }, false);
        return xhr;
      },

    }).done(parameters.done).error(parameters.error);
  }



  this.each(function(){
    var $this = $(this);

    $this.find('.dndAlternative').on('click',function(e){
      e.preventDefault();
      $this.find('input[type="file"]').trigger('click');
    });

    $this.find('input[type="file"]').on('change',function(){
      fd = new FormData();
      fd.append('file', $(this)[0].files[0]);
			fd.append('run','downsample');
      //upload(fd);
    });


    $this.on({
      dragcenter : function(e){
        e.preventDefault();
      },
      dragover : function(e){
        e.preventDefault();
        $this.addClass('hover');
      },
      dragleave : function(e){
        e.preventDefault();
        e.stopImmediatePropagation();
        $this.removeClass('hover');
      },
      drop : function(e){
        e.preventDefault();

        $this.removeClass('hover');

        var files = e.originalEvent.dataTransfer.files;

        fd = new FormData();
        fd.append('data', files[0]);

        if($.inArray(files[0].type,parameters.supported) < 0){
          alert(parameters.formatAlert);
          return false;
        }

        if(files[0].size > parameters.size*1038336 ){
          alert(parameters.sizeAlert);
          return false;
        }

        upload(fd);
      }
    });
  });
}


$('#dnd').dragAndDrop({
  'done' : function(response){
    $('#dnd .start, #dnd .error,#dnd progress').hide();
    $('#dnd .done').show();
		// var blb = new Blob([msg], {type: 'image/png'});
    // var url = (window.URL || window.webkitURL).createObjectURL(blb);
		// var blob = new Blob([msg], {type: 'image/jpeg'});
		//  var url = window.URL || window.webkitURL;
		$('#dnd .done').css('background','none')
		$('#dnd .done').html('<img id="responseImg" src="data:image/png;base64,'+response +'"/>');
		// document.getElementById("responseImg").src = dataURL;

    // console.info(msg);
  },
  'error' : function(){
    $('#dnd .start,#dnd .done,#dnd progress').hide();
    $('.error').show();
  },
  'onProgress' : function(p){
    $('#dnd .start,#dnd .done,#dnd .error').hide();
    $('#dnd progress').show().val(Math.round(p * 100));
  }

});
//downsample image click
$('#downsample').on('click', function() {
	//check the files
	var imageFile =  $('#fileElem').prop('files')[0];
	var uploadUrl= 'http://54.234.245.100:5000/xtH75NqswHKuu3fVKWJ4La'
	// console.log(imageFile, 'imageFile');
	if(!imageFile) {
		alert('Please upload an image');
	}
	var fd = new FormData();
	var supportedImages = ['image/jpg', 'image/jpeg'];
	fd.append('file', imageFile)
	if($.inArray(imageFile.type, supportedImages) < 0){
		alert('Format not supported');
		return false;
	}

	if(imageFile.size > 5*1038336 ){
		alert('File too heavy');
		return false;
	}
	fd.append('run','downsample');
	upload(fd, uploadUrl);
})


//repair image click
$('#repair').on('click', function() {
	//check the files
	var imageFile =  $('#fileElem').prop('files')[0];
	var uploadUrl= 'http://54.234.245.100:5000/enhance'
	// console.log(imageFile, 'imageFile');
	if(!imageFile) {
		alert('Please upload an image');
	}
	var fd = new FormData();
	var supportedImages = ['image/jpg', 'image/jpeg'];
	fd.append('file', imageFile)
	fd.append('model', 'repair')
	fd.append('zoom', '1')
	if($.inArray(imageFile.type, supportedImages) < 0){
		alert('Format not supported');
		return false;
	}

	if(imageFile.size > 5*1038336 ){
		alert('File too heavy');
		return false;
	}
	fd.append('run','downsample');
	upload(fd, uploadUrl);
})

//ehance image click
$('#enhancer-image').on('click', function() {
	//check the files
	var imageFile =  $('#fileElem').prop('files')[0];
	var uploadUrl= 'http://54.234.245.100:5000/ehance'
	// console.log(imageFile, 'imageFile');
	if(!imageFile) {
		alert('Please upload an image');
	}
	var fd = new FormData();
	var supportedImages = ['image/jpg', 'image/jpeg'];
	fd.append('file', imageFile)
	//fd.append('model', 'repair')
	fd.append('zoom', '1')
	if($.inArray(imageFile.type, supportedImages) < 0){
		alert('Format not supported');
		return false;
	}

	if(imageFile.size > 5*1038336 ){
		alert('File too heavy');
		return false;
	}
	fd.append('run','downsample');
	upload(fd, uploadUrl);
})

function upload(fd, url){
	// debugger;
	$.ajax({
		type: 'POST',
		url: url,
		data: fd,
		processData: false,
		contentType: false,
		xhr: function()
		{
			var xhr = new window.XMLHttpRequest();
			//  xhr.responseType= 'blob'
			xhr.upload.addEventListener("progress", function(evt){
				if (evt.lengthComputable) {
					var percentComplete = evt.loaded / evt.total;
					onProgress(percentComplete);
				}
			}, false);
			return xhr;
		},

	}).done(function(response){
		onDone(response)
	}).error(function(){
		onError()
	});
}

function onProgress(p){
	$('#dnd .start,#dnd .done,#dnd .error').hide();
	$('#dnd progress').show().val(Math.round(p * 100));
}

function onError(){
	$('#dnd .start,#dnd .done,#dnd progress').hide();
	$('.error').show();
}

function onDone(response){
	$('#dnd .start, #dnd .error,#dnd progress').hide();
	$('#dnd .done').show();
	// var blb = new Blob([msg], {type: 'image/png'});
	// var url = (window.URL || window.webkitURL).createObjectURL(blb);
	// var blob = new Blob([msg], {type: 'image/jpeg'});
	//  var url = window.URL || window.webkitURL;
	$('#dnd .done').css('background','none')
	$('#dnd .done').html('<img id="responseImg" src="data:image/png;base64,'+response +'"/>');
}
})
