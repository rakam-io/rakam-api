function wait_for_element(splash, css, maxwait)
  -- Wait until a selector matches an element
  -- in the page. Return an error if waited more
  -- than maxwait seconds.
  if maxwait == nil then
      maxwait = 10
  end
  return splash:wait_for_resume(string.format([[
    function main(splash) {
      var selector = '%s';
      var maxwait = %s;
      var end = Date.now() + maxwait*1000;

      function check() {
        if(document.querySelector(selector)) {
          splash.resume('Element found');
        } else if(Date.now() >= end) {
          var err = 'Timeout waiting for element';
          splash.error(err + " " + selector);
        } else {
          setTimeout(check, 200);
        }
      }
      check();
    }
  ]], css, maxwait))
end

function main(splash)
    local get_bbox = splash:jsfunc([[
        function(css) {
          var el = document.querySelector(css);
          var r = el.getBoundingClientRect();
          return [r.left, r.top, r.right, r.bottom];
        }
    ]])
    splash:add_cookie{"session", "{{session}}", "/", domain="{{domain}}"}
    splash:add_cookie{"active_project", "{{active_project}}", "/", domain="{{domain}}"}
    splash:set_viewport_size(1500, 1000)

    splash:go("http://{{domain}}{{path}}")
    wait_for_element(splash, ".dashboard-content.processing-done")

    splash:runjs([[
        var elem = document.getElementById('dashboard-content');
				elem.setAttribute('style', 'height: '+elem.style.height + ' !important');
    ]])
    splash:wait(1.5)
    splash:set_viewport_full()
    local region = get_bbox("#dashboard-content")
    return splash:png{region=region}
end

