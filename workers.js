// Service Worker format - no 'export default' needed
addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
  const url = new URL(request.url);
  const path = url.pathname;

  // Handle CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      },
    });
  }

  // Home route
  if (path === '/') {
    return new Response(
      JSON.stringify({
        name: "Instagram Profile API",
        endpoint: "/info?username=abbas_devs",
        method: "GET",
        created_by: "@abbas_devs"
      }),
      {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
        },
      }
    );
  }

  // Info route
  if (path === '/info') {
    const username = url.searchParams.get('username');

    if (!username) {
      return new Response(
        JSON.stringify({ error: "Username parameter is required" }),
        {
          status: 400,
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
          },
        }
      );
    }

    try {
      // Fetch raw data from Instagram API
      const response = await fetch(
        "https://europe-west3-storyviewer-7a64d.cloudfunctions.net/getInstagramData",
        {
          method: 'POST',
          headers: {
            'User-Agent': "okhttp/4.10.0",
            'Content-Type': "application/json; charset=utf-8",
          },
          body: JSON.stringify({
            data: {
              endpoint: "/v1/info",
              params: {
                include_about: true,
                username_or_id_or_url: username,
              },
            },
          }),
        }
      );

      if (!response.ok) {
        throw new Error(`API responded with status: ${response.status}`);
      }

      const rawData = await response.json();
      
      // Format the response
      const formattedData = formatResponse(rawData);

      return new Response(
        JSON.stringify(formattedData, null, 2),
        {
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
          },
        }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: error.message }),
        {
          status: 500,
          headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
          },
        }
      );
    }
  }

  // 404 for any other route
  return new Response(
    JSON.stringify({ error: "Not found" }),
    {
      status: 404,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    }
  );
}

function formatResponse(rawData) {
  // Extract data with safe navigation
  const about = (rawData && rawData.result && rawData.result.data && rawData.result.data.about) || {};
  const main = (rawData && rawData.result && rawData.result.data) || {};
  
  // Get profile pic URL
  const hdPic = about.profile_pic_url_hd || main.profile_pic_url_hd || '';
  const picInfo = about.hd_profile_pic_url_info || main.hd_profile_pic_url_info || {};

  return {
    "success": true,
    "data": {
      "username": about.username || main.username || '',
      "full_name": about.full_name || main.full_name || '',
      "bio": about.biography || main.biography || '',
      "profile_pic": hdPic,
      "profile_pic_width": picInfo.width || 0,
      "profile_pic_height": picInfo.height || 0,
      "followers": about.follower_count || main.follower_count || 0,
      "following": about.following_count || main.following_count || 0,
      "posts": main.media_count || 0,
      "user_id": about.id || main.id || '',
      "joined_date": about.date_joined || '',
      "former_usernames": about.former_usernames || 0,
      "category": main.category || 'Personal',
      "external_url": main.external_url || '',
      "country": about.country || main.country || '',
      "can_send_dm": main.can_send_direct_message || false,
      "meta_verified_eligible": main.is_eligible_for_meta_verified_label || false
    }
  };
}